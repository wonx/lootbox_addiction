
# Import Required Modules
from flask import Flask, render_template
import pandas as pd
import json
import plotly
import plotly.express as px
 
# Create Home Page Route
app = Flask(__name__)

# Import pickle dataframes
df_purchases_analytic_predictions =  pd.read_pickle('./df_pickles/df_purchases_analytic_predictions.pkl')
df_purchases_dailyaggregate =  pd.read_pickle('./df_pickles/df_purchases_dailyaggregate.pkl')
df_purchases_daily = df_purchases_dailyaggregate.groupby('date').agg({'Turnover':'sum', 'Hold': 'sum', 'NumberofBets': 'count'}).reset_index()
df_purchases_value =  pd.read_pickle('./df_pickles/df_purchases_value.pkl')


@app.route('/')
def bar_with_plotly():
     
    # Create Bar chart 1
    fig1 = px.line(df_purchases_daily, x='date', y='NumberofBets', title='Number of bets per day')

    # Create Bar chart 2
    #fig2 = px.bar(df, x='City', y='Age', color='Country', barmode='group')
    fig2 = px.line(df_purchases_daily, x='date', y='Hold', title='Hold per day', color_discrete_sequence=["#ff97ff"])

    # Create graphJSONs
    graphJSON1 = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graphJSON2 = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

    dict_userpredictions = df_purchases_analytic_predictions[['user', 'confidence_score']].sort_values(by='confidence_score', ascending=False).head(100).to_dict("records")
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
