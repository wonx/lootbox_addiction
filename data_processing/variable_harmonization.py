## Harmonizes variables between the online gambling dataset and the lootbox dataset
##  Basically transforms the df_purchases to a daily aggregate, and from that daily aggregate, 
##  an analytic dataset will be computed, with one row per user and 9 features.
##  On that analytic dataset, the machine learning model will be applied.
##  The 9 features are: UserID    sum_stakes_fixedodds    sum_bets_fixedodds    bettingdays_fixedodds    duration_fixedodds    frequency_fixedodds    bets_per_day_fixedodds    euros_per_bet_fixedodds    net_loss_fixedodds    percent_lost_fixedodds


import pandas as pd
import numpy as np
import datetime


def get_dailyaggregate(df_purchases_value):
    
    # Calculating Turnover and Hold features
    df_purchases_value['Turnover'] = df_purchases_value['src_value'] # all the money bet before any winnings are paid out or losses incurred. It's the same as the src_value then?
    df_purchases_value['Hold'] =  df_purchases_value['src_value']  - df_purchases_value['out_value'] # Total amount lost. Negative values mean the user won money.

    df_purchases_dailyaggregate = df_purchases_value.groupby(['date', 'user'], as_index=False).agg({'Turnover': 'sum', 'Hold': 'sum'})
    df_purchases_dailyaggregate['NumberofBets'] = df_purchases_value.groupby(['date', 'user']).size().reset_index(name='NumberofBets')['NumberofBets'] # Creates the column NumberofBets, by checking the size of the groupby of date and user.

    #display(df_purchases_dailyaggregate.head(10))
    return(df_purchases_dailyaggregate)


def get_analytic_dataset(df_purchases_dailyaggregate, datelimit=datetime.date.today()):
    # New empty dataframe
    df_purchases_analytic = pd.DataFrame()
    
    # Convert date feature to datetime
    df_purchases_dailyaggregate['date'] = pd.to_datetime(df_purchases_dailyaggregate['date'])
    
    # Only keep those values before the date limit
    df_purchases_dailyaggregate = df_purchases_dailyaggregate[df_purchases_dailyaggregate['date'] < datelimit]

    # 1. sum_stakes_fixedodds (sum of turnovers?)
    df_purchases_analytic['sum_stakes_fixedodds'] = df_purchases_dailyaggregate.groupby('user')['Turnover'].sum()

    # 2. sum_bets_fixedodds (sum of all the bets for all the days)
    df_purchases_analytic['sum_bets_fixedodds'] = df_purchases_dailyaggregate.groupby('user')['NumberofBets'].sum()

    # 3. bettingdays_fixedodds
    df_purchases_analytic['bettingdays_fixedodds'] = df_purchases_dailyaggregate.groupby('user')['NumberofBets'].count() # Make sure there are no days with users with 0 purchases

    # 4. duration_fixedodds (the number of days between the last and first date, both included)
    grouped = df_purchases_dailyaggregate.groupby('user')
    min_dates = grouped['date'].min()
    max_dates = grouped['date'].max()
    duration_fixedodds = (max_dates - min_dates).dt.days + 1
    df_purchases_analytic['duration_fixedodds'] = duration_fixedodds
    del min_dates, max_dates, grouped, duration_fixedodds

    # 5. frequency_fixedodds (percent of active days within the duration of gambling involvement) (bettingdays / duration)
    df_purchases_analytic['frequency_fixedodds'] = df_purchases_analytic['bettingdays_fixedodds'] / df_purchases_analytic['duration_fixedodds'] 

    # 6. bets_per_day_fixedodds
    df_purchases_analytic['bets_per_day_fixedodds'] = df_purchases_dailyaggregate.groupby('user')['NumberofBets'].mean()

    # 7. euros_per_bet_fixedodds (total monies wagered / total number of bets)
    df_purchases_analytic['euros_per_bet_fixedodds'] = df_purchases_analytic['sum_stakes_fixedodds'] / df_purchases_analytic['sum_bets_fixedodds']

    # 8. net_loss_fixedodds (sum of 'Hold')
    df_purchases_analytic['net_loss_fixedodds'] = df_purchases_dailyaggregate.groupby('user')['Hold'].sum()

    # 9. percent_lost_fixedodds (sum of 'Hold' / sum of 'Turnover')
    df_purchases_analytic['percent_lost_fixedodds'] = df_purchases_analytic['net_loss_fixedodds'] / df_purchases_analytic['sum_stakes_fixedodds']

    # Finally, reset index to the user is not the index.
    df_purchases_analytic = df_purchases_analytic.reset_index()

    return df_purchases_analytic


def get_sundays(start_date, end_date):
    # convert the start and end dates to datetime objects
    start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    
    # calculate the first Sunday following the start date
    sunday = start_datetime + datetime.timedelta(days=(6 - start_datetime.weekday()))
    
    # create a list to store the Sunday dates
    sundays = []
    
    # loop through the Sundays between the start and end dates
    while sunday <= end_datetime:
        sundays.append(sunday.strftime('%Y-%m-%d'))
        sunday += datetime.timedelta(days=7)
    
    return sundays


def generate_weekly_analytics():
    # Generate an analytic dataset for each week until the present
    startdate = df_purchases_dailyaggregate['date'].min().strftime('%Y-%m-%d')
    enddate = df_purchases_dailyaggregate['date'].max().strftime('%Y-%m-%d')
    endweekdates = get_sundays(str(startdate), str(enddate))

    for date in endweekdates:
        print("Generating analytic dataset for ending date:", date)
        print(get_analytic_dataset(df_purchases_dailyaggregate, datelimit=date).shape)
        df_purchases_analytic = get_analytic_dataset(df_purchases_dailyaggregate, datelimit=date)
        df_purchases_analytic.to_pickle(f'../processed_dataframes/analytic/{date}_df_purchases_analytic.pkl')



if __name__ == '__main__':
        
    # Import df_purchases_value dataframe
    print("Importing df_purchases_value dataframe...")
    df_purchases_value = pd.read_pickle('../processed_dataframes/df_purchases_value.pkl')
    #print(df_purchases_value.sample(4))
    print(df_purchases_value.shape)
    print(df_purchases_value.info())
   

    # Preparing dataset
    print("Shaping dataframe (getting rid of unused features)...")
    # Let's get rid of all non-relevant features for now
    df_purchases_value['date'] = df_purchases_value['datetime_zh'].dt.floor('d') # keep just the date, not the time for each purchase
    df_purchases_value = df_purchases_value[['date', 'user', 'src_value', 'out_value']]
    #print(df_purchases_value.sample(5))
    
    print(df_purchases_value.shape)
    print(df_purchases_value.info())

    # Convert out_value to float
    df_purchases_value['out_value'] = df_purchases_value['out_value'].str.replace(' ', '') # A few values appear as '1 327.00' instead of '1327.00'
    df_purchases_value['out_value'] = df_purchases_value['out_value'].replace('', np.nan) # There could be empty strings too
    df_purchases_value['out_value'] = df_purchases_value['out_value'].astype(float) # Convert out_value to float

    # Remove null rows (we lose ~6.6% of the rows)
    df_purchases_value.dropna(subset=['src_value', 'out_value'], inplace=True)

    print(df_purchases_value.info())
    print(df_purchases_value.shape)
    
    print("Generating df_purchases_dailyaggregate...")
    df_purchases_dailyaggregate = get_dailyaggregate(df_purchases_value)

    print(df_purchases_dailyaggregate.tail(5))
    
    # Save to pickle
    print("Saving df_purchases_dailyaggregate to pickle file...")
    df_purchases_dailyaggregate.to_pickle('../processed_dataframes/df_purchases_dailyaggregate.pkl')
    
    # Generate weekly analytic datasets
    print("Generating weekly analytic datasets...")
    generate_weekly_analytics()
    
    # Generate whole analytic dataset (kinda redundant...)
    print("Generating analytic dataset for the whole data..."
    df_purchases_analytic = get_analytic_dataset(df_purchases_dailyaggregate)
    df_purchases_analytic.to_pickle('../processed_dataframes/df_purchases_analytic.pkl')
    
    print("All done!")
    