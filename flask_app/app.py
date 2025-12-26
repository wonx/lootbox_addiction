
# Import Required Modules
from flask import Flask, render_template
import pandas as pd
import json
import plotly
import plotly.express as px
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import pytz # For time zones
import helpers
import os

 
# Create Home Page Route
app = Flask(__name__)

def import_dataframes():
    # Import pickle dataframes
    global totalpurchases
    global uniqueusers
    global onepercent
    global df_purchases
    global df_purchases_week
    global df_purchases_analytic_predictions
    global df_purchases_dailyaggregate
    global df_purchases_daily
    global df_purchases_value
    global df_byminute_interpolated_limit
    global df_top_users
    global df_purchases_analytic_predictions_date
    
    
    print("Refreshing dataframes...")
    
    # Determine the date range (last 45 days for retention, but will filter as needed)
    enddate = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai'))
    enddate = enddate.strftime("%Y-%m-%d %H:%M:%S")
    startdate = datetime.datetime.strptime(enddate, '%Y-%m-%d %H:%M:%S')
    cutoff_date = startdate - datetime.timedelta(days=45)  # Keep last 45 days in memory
    cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
    startdate -= datetime.timedelta(seconds=604800) # a week ago (604800s) for display
    startdate = startdate.strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"Loading data from {cutoff_date_str} onwards (last 45 days)")
    
    # Importing raw purchases dataset
    df_purchases = pd.read_pickle("../processed_dataframes/df_purchases.pkl")
    
    # Convert df_purchases to Shanghai timezone and filter to last 45 days
    df_purchases['datetimeUTC'] = pd.to_datetime(df_purchases['timestamp'], unit='s')
    df_purchases['datetime'] = df_purchases['datetimeUTC'].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    df_purchases.set_index('datetime', inplace=True)
    
    # Filter to last 45 days
    df_purchases = df_purchases[df_purchases.index.date >= cutoff_date.date()]
    
    # Calculate statistics on the filtered data
    totalpurchases = df_purchases.shape[0]
    uniqueusers = df_purchases['user'].nunique()
    
    df_purchases_grouped = df_purchases.groupby('user').count()
    n_users = int(len(df_purchases_grouped) * 0.01)
    onepercent = round(df_purchases_grouped['timestamp'].nlargest(n_users).mean(), 1) if n_users > 0 else 0
    
    # Only keep the period between now and a week ago for display purposes
    df_purchases_week = helpers.get_df_period(df_purchases, startdate, enddate)

    
    # Aggregate and interpolate df_purchases_week (last 7 days) for the graphs
    df_by_second_interpolated = helpers.get_df_bysecond_interpolated(df_purchases_week, startdate, enddate)
    df_byminute_interpolated_limit = df_by_second_interpolated.resample("5T").sum()
    
    # Dataframe for the heatmap (based on filtered df_purchases)
    df_top_users = helpers.get_df_top_users(df_purchases_week)
    
    # Load and filter aggregated dataframes
    df_purchases_analytic_predictions = pd.read_pickle('../processed_dataframes/df_purchases_analytic_predictions.pkl')
    
    df_purchases_analytic_predictions_date = pd.read_pickle('../processed_dataframes/df_purchases_analytic_predictions_date.pkl')
    # Filter to last 45 days
    if 'date' in df_purchases_analytic_predictions_date.columns:
        df_purchases_analytic_predictions_date['date'] = pd.to_datetime(df_purchases_analytic_predictions_date['date'])
        df_purchases_analytic_predictions_date = df_purchases_analytic_predictions_date[df_purchases_analytic_predictions_date['date'].dt.date >= cutoff_date.date()]
    
    # Load FULL df_purchases_dailyaggregate for user detail pages (don't filter)
    # This allows viewing historical data for users with no recent purchases
    df_purchases_dailyaggregate = pd.read_pickle('../processed_dataframes/df_purchases_dailyaggregate.pkl')
    if 'date' in df_purchases_dailyaggregate.columns:
        df_purchases_dailyaggregate['date'] = pd.to_datetime(df_purchases_dailyaggregate['date'])
    
    df_purchases_daily = df_purchases_dailyaggregate.groupby('date').agg({'Turnover':'sum', 'Hold': 'sum', 'NumberofBets': 'count'}).reset_index()
    
    # Load and filter df_purchases_value (only last 45 days)
    df_purchases_value = pd.read_pickle('../processed_dataframes/df_purchases_value.pkl')
    if 'datetime_zh' in df_purchases_value.columns:
        df_purchases_value['datetime_zh'] = pd.to_datetime(df_purchases_value['datetime_zh'])
        df_purchases_value = df_purchases_value[df_purchases_value['datetime_zh'].dt.date >= cutoff_date.date()]
    elif 'timestamp' in df_purchases_value.columns:
        df_purchases_value['datetime_zh'] = pd.to_datetime(df_purchases_value['timestamp'], unit='s').dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
        df_purchases_value = df_purchases_value[df_purchases_value['datetime_zh'].dt.date >= cutoff_date.date()]
    
    
    globals_vars = [
        'totalpurchases',
        'uniqueusers',
        'onepercent',
        'df_purchases',
        'df_purchases_week',
        'df_purchases_analytic_predictions',
        'df_purchases_dailyaggregate',
        'df_purchases_daily',
        'df_purchases_value',
        'df_byminute_interpolated_limit',
        'df_top_users',
        'df_purchases_analytic_predictions_date'
    ]

    print("\n=== Memory Usage Report ===")
    total_memory = 0
    for var_name in globals_vars:
        var = globals()[var_name]
        size = var.memory_usage(deep=True).sum() if hasattr(var, 'memory_usage') else 0
        total_memory += size
        print(f"  {var_name}: {size / (1024 ** 2):.2f} MB")
    print(f"  TOTAL: {total_memory / (1024 ** 3):.2f} GB")
    print("===========================\n")

import_dataframes()

# Start scheduled task 
scheduler = BackgroundScheduler()
scheduler.add_job(import_dataframes, 'interval', minutes=10)
scheduler.start()


@app.route('/')
def bar_with_plotly():
     
    # Purchases during last week
    #fig1 = px.line(df_purchases_daily, x='date', y='NumberofBets', title='Number of bets per day')
    fig1 = px.line(df_byminute_interpolated_limit, 
                   x=df_byminute_interpolated_limit.index, 
                   y='out', 
                   title='', 
                   color_discrete_sequence=["#ff9900"],
                   labels={
                     "out": "Amount of purchases (5m agg.)",
                     "index": ""})
    fig1.update_layout({
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'font_color': 'white'
    })
    
    # Line width
    fig1.update_traces(patch={"line": 
                                  {"width": 1.3}
                             })


    # Heatmap
    fig2 = px.imshow(df_top_users, 
                     color_continuous_scale='Thermal', 
                     labels={
                         "y": "Purchases per day"})
    fig2.update_layout({
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'font_color': 'white',
        'yaxis': {'side': 'left'}
    })

    # Create graphJSONs
    graphJSON1 = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graphJSON2 = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    dict_userpredictions = df_purchases_analytic_predictions[['user', 'confidence_score', 'improving']].sort_values(by='confidence_score', ascending=False).head(150).to_dict("records")
    
    # Convert -1, 0 and 1 to triangles
    for record in dict_userpredictions:
        record['improving'] = helpers.get_arrow(record['improving'])
    
    
    # Round to 3 decimal points
    for d in dict_userpredictions:
        d['confidence_score'] = round(d['confidence_score']*100, 1)
        
    # Get last modification date of df_purchases
    last_update = helpers.get_last_modified_date("../processed_dataframes/df_purchases.pkl")
    
    # Check if lockfile is present
    lockfile_path = "../processed_dataframes/df_purchases_value.lock"
    lockfile_exists = os.path.isfile(lockfile_path)
    print("lockfile: ", lockfile_exists)
    
    # Stat boxes
    #totalpurchases = 0
    #uniqueusers = 0
    purchasesperuser = round(totalpurchases / uniqueusers, 1) if uniqueusers > 0 else 0
    purchasesweek = df_purchases_week.shape[0]
    usersatrisk = df_purchases_analytic_predictions[df_purchases_analytic_predictions['addiction'] == 1].shape[0]
    daysofdata = df_purchases_dailyaggregate.groupby('date').count().shape[0]
    
    #onepercent = 0
    

    # Use render_template to pass graphJSON to html
    return render_template('bar.html', graphJSON1=graphJSON1, graphJSON2=graphJSON2, users=dict_userpredictions, last_update=last_update, lockfile_exists=lockfile_exists, totalpurchases=totalpurchases, purchasesperuser=purchasesperuser, purchasesweek=purchasesweek, uniqueusers=uniqueusers, usersatrisk=usersatrisk, daysofdata=daysofdata, onepercent=onepercent)
 
# Create User Page Route
@app.route('/user/<user>')
def user_page(user):
    
    # Force the bar graph to show dates until today
    today = datetime.date.today()
    df = df_purchases_dailyaggregate[df_purchases_dailyaggregate['user'] == user]
    
    # Handle empty dataframe and NaT values
    if len(df) > 0 and pd.notna(df['date'].max()):
        if df['date'].max().date() < today:
            new_row = pd.DataFrame({'date': [today], 'value': [0]})
            df = pd.concat([df, new_row], ignore_index=True)
    elif len(df) == 0:
        # User has no data, return empty page gracefully
        return render_template('user.html', user=user, graphJSON_user=json.dumps({}, cls=plotly.utils.PlotlyJSONEncoder), 
                               graphJSON_timeday=json.dumps({}, cls=plotly.utils.PlotlyJSONEncoder), userpurchases=[], 
                               riskscore=0, last_update="N/A", firstpurchase="N/A", totalspent=0, totalbets=0, betfrequency=0)

    # Generate the plot using the username
    #  purchases per day
    fig1 = px.bar(df, 
                  x='date', 
                  y='NumberofBets', 
                  title='Lootbox purchases per day', 
                  color_discrete_sequence=["#ff9900"],
                  labels={
                     "date": "",
                     "NumberofBets": ""})
    fig1.update_layout({
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'font_color': 'white',
        'hovermode': "x"
    })
    
    # Risk score evolution
    userfilter = df_purchases_analytic_predictions_date['user'] == user
    risk_score_evolution = df_purchases_analytic_predictions_date[userfilter].sort_values(by='date')
    
    #  purchases by time of day
    #purchase_by_hour = df_purchases_value[df_purchases_value['user']  == user].groupby([df_purchases_value['datetime_zh'].dt.hour], as_index=False).count().reset_index()
    
    fig2 = px.line(risk_score_evolution, 
                  x='date', 
                  y='confidence_score', 
                  title='Risk score evolution', 
                  color_discrete_sequence=["red"],
                  labels={
                     "date": "",
                     "confidence_score": ""})
    
    fig2.update_layout({
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'font_color': 'white',
        'hovermode': "x"
    })

    # Create graphJSONs
    graphJSON_user = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graphJSON_timeday = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    # Table with the purchases for that user
    dict_userpurchases = df_purchases_dailyaggregate[df_purchases_dailyaggregate['user'] == user].sort_values(by='date', ascending=False)
    dict_userpurchases['date'] = dict_userpurchases['date'].dt.strftime('%Y-%m-%d')
    dict_userpurchases = dict_userpurchases.to_dict("records")

    # Stats for that user
    try:
        # Last risk score
        riskscore = round(df_purchases_analytic_predictions['confidence_score'][df_purchases_analytic_predictions['user'] == user].item()*100, 1)
        # Round to 3 decimal points
        for d in dict_userpurchases:
            d['Turnover'] = round(d['Turnover'], 2)
            d['Hold'] = round(d['Hold'], 2)
            
        # First purchase
        firstpurchase = df_purchases_dailyaggregate['date'][df_purchases_dailyaggregate['user'] == user].min().strftime('%Y-%m-%d')
        # Total spent
        totalspent = df_purchases_dailyaggregate['Turnover'][df_purchases_dailyaggregate['user'] == user].sum()
        totalspent = round(totalspent,2)
        
        # Total bets
        totalbets = df_purchases_dailyaggregate['NumberofBets'][df_purchases_dailyaggregate['user'] == user].sum()
        
        # Bet frequency
        betfrequency = df_purchases_analytic_predictions['frequency_fixedodds'][df_purchases_analytic_predictions['user'] == user].sum()*100
        betfrequency = round(betfrequency, 1)
        
    except ValueError: # Bug #2: risk score still not computed for that user
        riskscore = 0
        totalspent = 0
        totalbets = 0
        betfrequency = 0
        firstpurchase = "N/A"
        pass
        
    print("Riskscore:", riskscore)
    
    
    
    # Get last modification date of df_purchases_dailyaggregate
    last_update = helpers.get_last_modified_date("../processed_dataframes/df_purchases_dailyaggregate.pkl")

    return render_template('user.html', user=user, graphJSON_user=graphJSON_user, graphJSON_timeday=graphJSON_timeday, userpurchases=dict_userpurchases, riskscore=riskscore, last_update=last_update, firstpurchase=firstpurchase, totalspent=totalspent, totalbets=totalbets, betfrequency=betfrequency)

# User purchases per date, raw data
@app.route('/user/<user>/<date>')
def user_date(user, date):
    
    # Filter the data for the specific user and date
    user_date_data = df_purchases_value[(df_purchases_value['datetime_zh'].dt.floor('d') == date) & (df_purchases_value['user']  == user)].to_dict("records")
    
    # If empty, load the full pickle on-demand (avoids keeping full df_purchases_value in memory)
    if len(user_date_data) == 0:
        full_path = '../processed_dataframes/df_purchases_value.pkl'
        if os.path.isfile(full_path):
            try:
                df_full = pd.read_pickle(full_path)
                # ensure datetime_zh exists
                if 'datetime_zh' not in df_full.columns:
                    if 'timestamp' in df_full.columns:
                        df_full['datetime_zh'] = pd.to_datetime(df_full['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
                user_date_data = df_full[(df_full['datetime_zh'].dt.floor('d') == date) & (df_full['user'] == user)].to_dict('records')
            except Exception:
                user_date_data = []
    
    # Get last modification date of df_purchases_value
    last_update = helpers.get_last_modified_date("../processed_dataframes/df_purchases_value.pkl")

    return render_template('userpurchases.html', user=user, date=date, user_date_data=user_date_data, last_update=last_update)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9018, debug=True)
