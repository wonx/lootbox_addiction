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
import duckdb  # <-- NEW! For out-of-core memory management

app = Flask(__name__)

def import_dataframes():
    global totalpurchases, uniqueusers, onepercent, df_purchases
    global df_purchases_week, df_purchases_analytic_predictions
    global df_purchases_dailyaggregate, df_purchases_daily
    global df_purchases_value, df_byminute_interpolated_limit
    global df_top_users, df_purchases_analytic_predictions_date
    
    print("Refreshing dataframes using DuckDB...")
    
    # Connect DuckDB to query the hard drive natively 
    con = duckdb.connect()
    
    enddate = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai'))
    enddate = enddate.strftime("%Y-%m-%d %H:%M:%S")
    startdate = datetime.datetime.strptime(enddate, '%Y-%m-%d %H:%M:%S')
    cutoff_date = startdate - datetime.timedelta(days=45)
    cutoff_timestamp = cutoff_date.timestamp() # for columns that are unix epoch
    cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
    
    startdate -= datetime.timedelta(seconds=604800)
    startdate = startdate.strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"Loading data from {cutoff_date_str} onwards (last 45 days)")
    
    # 1. df_purchases (Huge file!)
    # Get total stats natively via SQL (0 MB RAM used!)
    totalpurchases = con.execute("SELECT COUNT(*) FROM '../processed_dataframes/df_purchases.parquet'").fetchone()[0]
    uniqueusers = con.execute("SELECT COUNT(DISTINCT user) FROM '../processed_dataframes/df_purchases.parquet'").fetchone()[0]
    
    user_counts = con.execute("SELECT COUNT(*) as cnt FROM '../processed_dataframes/df_purchases.parquet' GROUP BY user").df()
    n_users = int(len(user_counts) * 0.01)
    onepercent = round(user_counts['cnt'].nlargest(n_users).mean(), 1) if n_users > 0 else 0
    
    # Load ONLY the last 45 days into memory using the unix timestamp
    df_purchases = con.execute(f"SELECT * FROM '../processed_dataframes/df_purchases.parquet' WHERE timestamp >= {cutoff_timestamp}").df()
    
    # Apply the timezone only to the 45-day chunk
    df_purchases['datetimeUTC'] = pd.to_datetime(df_purchases['timestamp'], unit='s')
    df_purchases['datetime'] = df_purchases['datetimeUTC'].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    df_purchases.set_index('datetime', inplace=True)
    
    df_purchases_week = helpers.get_df_period(df_purchases, startdate, enddate)
    df_by_second_interpolated = helpers.get_df_bysecond_interpolated(df_purchases_week, startdate, enddate)
    df_byminute_interpolated_limit = df_by_second_interpolated.resample("5min").sum()
    df_top_users = helpers.get_df_top_users(df_purchases_week)
    
    # 2. df_purchases_analytic_predictions
    df_purchases_analytic_predictions = pd.read_parquet('../processed_dataframes/df_purchases_analytic_predictions.parquet')
    
    # 3. df_purchases_analytic_predictions_date
    df_purchases_analytic_predictions_date = con.execute(f"SELECT * FROM '../processed_dataframes/df_purchases_analytic_predictions_date.parquet' WHERE CAST(date AS DATE) >= '{cutoff_date_str}'").df()
    if 'date' in df_purchases_analytic_predictions_date.columns:
        df_purchases_analytic_predictions_date['date'] = pd.to_datetime(df_purchases_analytic_predictions_date['date'])
    
    # 4. df_purchases_dailyaggregate
    df_purchases_dailyaggregate = pd.read_parquet('../processed_dataframes/df_purchases_dailyaggregate.parquet')
    if 'date' in df_purchases_dailyaggregate.columns:
        df_purchases_dailyaggregate['date'] = pd.to_datetime(df_purchases_dailyaggregate['date'])
    
    df_purchases_daily = df_purchases_dailyaggregate.groupby('date').agg({'Turnover':'sum', 'Hold': 'sum', 'NumberofBets': 'count'}).reset_index()
    
    # 5. df_purchases_value (Huge file! Load only 45 days via SQL)
    df_purchases_value = con.execute(f"SELECT * FROM '../processed_dataframes/df_purchases_value.parquet' WHERE timestamp >= {cutoff_timestamp}").df()
    if 'timestamp' in df_purchases_value.columns:
        df_purchases_value['datetime_zh'] = pd.to_datetime(df_purchases_value['timestamp'], unit='s').dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    
    globals_vars =[
        'df_purchases', 'df_purchases_week', 'df_purchases_analytic_predictions',
        'df_purchases_dailyaggregate', 'df_purchases_daily', 'df_purchases_value',
        'df_byminute_interpolated_limit', 'df_top_users', 'df_purchases_analytic_predictions_date'
    ]

    print("\n=== Memory Usage Report ===")
    total_memory = 0
    for var_name in globals_vars:
        var = globals()[var_name]
        size = var.memory_usage(deep=True).sum() if hasattr(var, 'memory_usage') else 0
        total_memory += size
        print(f"  {var_name}: {size / (1024 ** 2):.2f} MB")
    print(f"  TOTAL: {total_memory / (1024 ** 2):.2f} MB")
    print("===========================\n")

import_dataframes()

# Start scheduled task 
scheduler = BackgroundScheduler()
scheduler.add_job(import_dataframes, 'interval', minutes=10)
scheduler.start()


@app.route('/')
def bar_with_plotly():
    fig1 = px.line(df_byminute_interpolated_limit, 
                   x=df_byminute_interpolated_limit.index, 
                   y='out', 
                   title='', 
                   color_discrete_sequence=["#ff9900"],
                   labels={"out": "Amount of purchases (5m agg.)", "index": ""})
    fig1.update_layout({'plot_bgcolor': 'rgba(0,0,0,0)', 'paper_bgcolor': 'rgba(0,0,0,0)', 'font_color': 'white'})
    fig1.update_traces(patch={"line": {"width": 1.3}})

    fig2 = px.imshow(df_top_users, color_continuous_scale='Thermal', labels={"y": "Purchases per day"})
    fig2.update_layout({'plot_bgcolor': 'rgba(0,0,0,0)', 'paper_bgcolor': 'rgba(0,0,0,0)', 'font_color': 'white', 'yaxis': {'side': 'left'}})

    graphJSON1 = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graphJSON2 = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    dict_userpredictions = df_purchases_analytic_predictions[['user', 'confidence_score', 'improving']].sort_values(by='confidence_score', ascending=False).head(150).to_dict("records")
    for record in dict_userpredictions:
        record['improving'] = helpers.get_arrow(record['improving'])
    for d in dict_userpredictions:
        d['confidence_score'] = round(d['confidence_score']*100, 1)
        
    last_update = helpers.get_last_modified_date("../processed_dataframes/df_purchases.parquet")
    lockfile_exists = os.path.isfile("../processed_dataframes/df_purchases_value.lock")
    
    purchasesperuser = round(totalpurchases / uniqueusers, 1) if uniqueusers > 0 else 0
    purchasesweek = df_purchases_week.shape[0]
    usersatrisk = df_purchases_analytic_predictions[df_purchases_analytic_predictions['addiction'] == 1].shape[0]
    daysofdata = df_purchases_dailyaggregate.groupby('date').count().shape[0]
    
    return render_template('bar.html', graphJSON1=graphJSON1, graphJSON2=graphJSON2, users=dict_userpredictions, last_update=last_update, lockfile_exists=lockfile_exists, totalpurchases=totalpurchases, purchasesperuser=purchasesperuser, purchasesweek=purchasesweek, uniqueusers=uniqueusers, usersatrisk=usersatrisk, daysofdata=daysofdata, onepercent=onepercent)
 
@app.route('/user/<user>')
def user_page(user):
    today = datetime.date.today()
    df = df_purchases_dailyaggregate[df_purchases_dailyaggregate['user'] == user]
    if len(df) > 0 and pd.notna(df['date'].max()):
        if df['date'].max().date() < today:
            new_row = pd.DataFrame({'date': [today], 'value': [0]})
            df = pd.concat([df, new_row], ignore_index=True)
    elif len(df) == 0:
        return render_template('user.html', user=user, graphJSON_user=json.dumps({}, cls=plotly.utils.PlotlyJSONEncoder), graphJSON_timeday=json.dumps({}, cls=plotly.utils.PlotlyJSONEncoder), userpurchases=[], riskscore=0, last_update="N/A", firstpurchase="N/A", totalspent=0, totalbets=0, betfrequency=0)

    fig1 = px.bar(df, x='date', y='NumberofBets', title='Lootbox purchases per day', color_discrete_sequence=["#ff9900"])
    fig1.update_layout({'plot_bgcolor': 'rgba(0,0,0,0)', 'paper_bgcolor': 'rgba(0,0,0,0)', 'font_color': 'white', 'hovermode': "x"})
    
    userfilter = df_purchases_analytic_predictions_date['user'] == user
    risk_score_evolution = df_purchases_analytic_predictions_date[userfilter].sort_values(by='date')
    
    fig2 = px.line(risk_score_evolution, x='date', y='confidence_score', title='Risk score evolution', color_discrete_sequence=["red"])
    fig2.update_layout({'plot_bgcolor': 'rgba(0,0,0,0)', 'paper_bgcolor': 'rgba(0,0,0,0)', 'font_color': 'white', 'hovermode': "x"})

    graphJSON_user = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graphJSON_timeday = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    dict_userpurchases = df_purchases_dailyaggregate[df_purchases_dailyaggregate['user'] == user].sort_values(by='date', ascending=False)
    dict_userpurchases['date'] = dict_userpurchases['date'].dt.strftime('%Y-%m-%d')
    dict_userpurchases = dict_userpurchases.to_dict("records")

    try:
        riskscore = round(df_purchases_analytic_predictions['confidence_score'][df_purchases_analytic_predictions['user'] == user].item()*100, 1)
        for d in dict_userpurchases:
            d['Turnover'] = round(d['Turnover'], 2)
            d['Hold'] = round(d['Hold'], 2)
        firstpurchase = df_purchases_dailyaggregate['date'][df_purchases_dailyaggregate['user'] == user].min().strftime('%Y-%m-%d')
        totalspent = round(df_purchases_dailyaggregate['Turnover'][df_purchases_dailyaggregate['user'] == user].sum(), 2)
        totalbets = df_purchases_dailyaggregate['NumberofBets'][df_purchases_dailyaggregate['user'] == user].sum()
        betfrequency = round(df_purchases_analytic_predictions['frequency_fixedodds'][df_purchases_analytic_predictions['user'] == user].sum()*100, 1)
    except ValueError:
        riskscore = totalspent = totalbets = betfrequency = 0
        firstpurchase = "N/A"
        
    last_update = helpers.get_last_modified_date("../processed_dataframes/df_purchases_dailyaggregate.parquet")
    return render_template('user.html', user=user, graphJSON_user=graphJSON_user, graphJSON_timeday=graphJSON_timeday, userpurchases=dict_userpurchases, riskscore=riskscore, last_update=last_update, firstpurchase=firstpurchase, totalspent=totalspent, totalbets=totalbets, betfrequency=betfrequency)

@app.route('/user/<user>/<date>')
def user_date(user, date):
    # DUCKDB FIX: Instantly query the specific user/date from disk
    con = duckdb.connect()
    try:
        # Optimized query to filter by user AND date directly in SQL
        # This is much faster than loading all of a user's data
        query = f"SELECT * FROM '../processed_dataframes/df_purchases_value.parquet' WHERE user = '{user}' AND CAST(to_timestamp(timestamp) AS DATE) = '{date}'"
        
        df_user_date = con.execute(query).df()
        
        # We still need to add the timezone for display, but now only on a tiny dataframe
        if not df_user_date.empty:
            df_user_date['datetime_zh'] = pd.to_datetime(df_user_date['timestamp'], unit='s').dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
        
        user_date_data = df_user_date.to_dict('records')

    except Exception as e:
        print(f"Error querying specific user date: {e}")
        user_date_data = [] # <-- THE FIX IS HERE

    # Get last modification date of df_purchases_value
    last_update = helpers.get_last_modified_date("../processed_dataframes/df_purchases_value.parquet")

    return render_template('userpurchases.html', user=user, date=date, user_date_data=user_date_data, last_update=last_update)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9018, debug=True)