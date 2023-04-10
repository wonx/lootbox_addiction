
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
    global df_purchases_analytic_predictions
    global df_purchases_dailyaggregate
    global df_purchases_daily
    global df_purchases_value
    global df_byminute_interpolated_limit
    global df_top_users
    global df_purchases_analytic_predictions_date
    
    
    print("Refreshing dataframes...")
    df_purchases_analytic_predictions = pd.read_pickle('../processed_dataframes/df_purchases_analytic_predictions.pkl')
    df_purchases_analytic_predictions_date = pd.read_pickle('../processed_dataframes/df_purchases_analytic_predictions_date.pkl')
    df_purchases_dailyaggregate = pd.read_pickle('../processed_dataframes/df_purchases_dailyaggregate.pkl')
    df_purchases_daily = df_purchases_dailyaggregate.groupby('date').agg({'Turnover':'sum', 'Hold': 'sum', 'NumberofBets': 'count'}).reset_index()
    df_purchases_value =  pd.read_pickle('../processed_dataframes/df_purchases_value.pkl')
    
    # Determine the last week period
    #enddate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # right now, to string
    enddate = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai'))#.strftime("%Y-%m-%d %H:%M:%S")
    #enddate -= datetime.timedelta(seconds=1200) # substract the last 20 minutes, we might don't have data yet for that aggregate, so the graph doesn't go to 0 at the end
    enddate = enddate.strftime("%Y-%m-%d %H:%M:%S")
    startdate = datetime.datetime.strptime(enddate, '%Y-%m-%d %H:%M:%S')
    startdate -= datetime.timedelta(seconds=604800) # a week ago (604800s)
    startdate = startdate.strftime("%Y-%m-%d %H:%M:%S") # convert back to string
    
    # Importing raw purchases dataset and limiting to one week
    df_purchases = pd.read_pickle("../processed_dataframes/df_purchases.pkl")
    totalpurchases = df_purchases.shape[0] # for the statbox
    uniqueusers = df_purchases['user'].nunique()
    
    df_purchases_grouped = df_purchases.groupby('user').count() # for the onepercent top users
    n_users = int(len(df_purchases_grouped) * 0.01)
    onepercent = round(df_purchases_grouped['timestamp'].nlargest(n_users).mean(), 1)
    #df_purchases['datetime'] = pd.to_datetime(df_purchases['datetime'])

    # Set datetime as index
    #df_purchases.set_index('datetime', inplace=True)
    
    # Convert df_purchases to Shangai timezone
    df_purchases['datetimeUTC'] = pd.to_datetime(df_purchases['timestamp'], unit='s') # Will be in the UTC timezone by default
    df_purchases['datetime'] = df_purchases['datetimeUTC'].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    df_purchases.set_index('datetime', inplace=True) # Set datetime as index
    
    # Only keep the period between now and a week ago
    df_purchases = helpers.get_df_period(df_purchases, startdate, enddate) # this is completely redundant, as it's also in the helpers.py file. Can be optimized.


    # Aggregate and interpolate df_purchases by 5 minutes for the last week
    df_by_second_interpolated = helpers.get_df_bysecond_interpolated(df_purchases, startdate, enddate)
    df_byminute_interpolated_limit = df_by_second_interpolated.resample("5T").sum()
    
    # Dataframe for the heatmap
    df_top_users = helpers.get_df_top_users(df_purchases)
    
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
    purchasesperuser = round(totalpurchases / uniqueusers,1)
    purchasesweek = df_purchases.shape[0]
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
    if df['date'].max().date() < today:
        new_row = pd.DataFrame({'date': [today], 'value': [0]})
        df = pd.concat([df, new_row], ignore_index=True)

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
    
    # Get last modification date of df_purchases_value
    last_update = helpers.get_last_modified_date("../processed_dataframes/df_purchases_value.pkl")

    return render_template('userpurchases.html', user=user, date=date, user_date_data=user_date_data, last_update=last_update)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9018, debug=True)
