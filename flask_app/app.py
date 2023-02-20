
# Import Required Modules
from flask import Flask, render_template
import pandas as pd
import json
import plotly
import plotly.express as px
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import helpers

 
# Create Home Page Route
app = Flask(__name__)

def import_dataframes():
    # Import pickle dataframes
    global df_purchases_analytic_predictions
    global df_purchases_dailyaggregate
    global df_purchases_daily
    global df_purchases_value
    global df_byminute_interpolated_limit
    global df_top_users
    
    
    print("Refreshing dataframes...")
    df_purchases_analytic_predictions = pd.read_pickle('../processed_dataframes/df_purchases_analytic_predictions.pkl')
    df_purchases_dailyaggregate = pd.read_pickle('../processed_dataframes/df_purchases_dailyaggregate.pkl')
    df_purchases_daily = df_purchases_dailyaggregate.groupby('date').agg({'Turnover':'sum', 'Hold': 'sum', 'NumberofBets': 'count'}).reset_index()
    df_purchases_value =  pd.read_pickle('../processed_dataframes/df_purchases_value.pkl')
    
    # Determine the last week period
    enddate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # right now, to string
    startdate = datetime.datetime.strptime(enddate, '%Y-%m-%d %H:%M:%S')
    startdate -= datetime.timedelta(seconds=604800) # a day ago
    startdate = startdate.strftime("%Y-%m-%d %H:%M:%S") # convert back to string
    
    # Importing raw purchases dataset and limiting to one week
    df_purchases = pd.read_pickle("../processed_dataframes/df_purchases.pkl")
    df_purchases['datetime'] = pd.to_datetime(df_purchases['datetime'])
    df_purchases.set_index('datetime', inplace=True)
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
     
    # Create Bar chart 1
    #fig1 = px.line(df_purchases_daily, x='date', y='NumberofBets', title='Number of bets per day')
    fig1 = px.line(df_byminute_interpolated_limit, x=df_byminute_interpolated_limit.index, y='out', title='Total purchases (5m intervals)', labels={
                     "out": "Amount of purchases",
                     "index": ""},)

    # Create Bar chart 2
    #fig2 = px.line(df_purchases_daily, x='date', y='Hold', title='Hold per day', color_discrete_sequence=["#ff97ff"])
    fig2 = px.imshow(df_top_users)

    # Create graphJSONs
    graphJSON1 = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graphJSON2 = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    dict_userpredictions = df_purchases_analytic_predictions[['user', 'confidence_score', 'improving']].sort_values(by='confidence_score', ascending=False).head(100).to_dict("records")
    
    # Convert -1, 0 and 1 to triangles
    for record in dict_userpredictions:
        record['improving'] = helpers.get_arrow(record['improving'])
    
    
    # Round to 3 decimal points
    for d in dict_userpredictions:
        d['confidence_score'] = round(d['confidence_score']*100, 1)
    

    # Use render_template to pass graphJSON to html
    return render_template('bar.html', graphJSON1=graphJSON1, graphJSON2=graphJSON2, users=dict_userpredictions)
 
# Create User Page Route
@app.route('/user/<user>')
def user_page(user):

    # Generate the plot using the username
    #  purchases per day
    fig1 = px.bar(df_purchases_dailyaggregate[df_purchases_dailyaggregate['user'] == user], x='date', y='NumberofBets', title='Bets per day', color_discrete_sequence=["#5597ff"])
    #  purchases by time of day
    purchase_by_hour = df_purchases_value[df_purchases_value['user']  == user].groupby([df_purchases_value['datetime_zh'].dt.hour], as_index=False).count().reset_index()
    fig2 = px.bar(purchase_by_hour, x='index', y='out_value', title='Lootbox purchases per time of day', color_discrete_sequence=["#559733"])

    # Create graphJSONs
    graphJSON_user = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graphJSON_timeday = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    # Table with the purchases for that user
    dict_userpurchases = df_purchases_dailyaggregate[df_purchases_dailyaggregate['user'] == user].sort_values(by='date', ascending=False).to_dict("records")

    # Stats for that user
    riskscore = round(df_purchases_analytic_predictions['confidence_score'][df_purchases_analytic_predictions['user'] == user].item()*100, 1)
    # Round to 3 decimal points
    for d in dict_userpurchases:
        d['Turnover'] = round(d['Turnover'], 2)
        d['Hold'] = round(d['Hold'], 2)

    return render_template('user.html', user=user, graphJSON_user=graphJSON_user, graphJSON_timeday=graphJSON_timeday, userpurchases=dict_userpurchases, riskscore=riskscore)

# User purchases per date, raw data
@app.route('/user/<user>/<date>')
def user_date(user, date):

    # Filter the data for the specific user and date
    user_date_data = df_purchases_value[(df_purchases_value['datetime_zh'].dt.floor('d') == date) & (df_purchases_value['user']  == user)].to_dict("records")

    return render_template('userpurchases.html', user=user, date=date, user_date_data=user_date_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9018, debug=True)
