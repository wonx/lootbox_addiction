import pandas as pd
import numpy as np
import datetime
import pytz # For time zones

# To convert the 1,0,-1 values of the Improving feature to upward or downward triangles
def get_arrow(value):
    if value == 1:
        return '▼'
    elif value == 0:
        return '='
    elif value == -1:
        return '▲'
    else:
        return ''


# Interpolates values in a dataframe that only when there's more than x (limit_low) consecutive NaN but less than limit_high consecutive Nan
def interpolate_limit(df, limit_low, limit_high):
    df = df.copy(deep=True) # so it doesn't modify the original df
    notnull = pd.notnull(df).all(axis=1)
    # assign group numbers to the rows of df. Each group starts with a non-null row,
    # followed by null rows
    group = notnull.cumsum()
    # find the index of groups having length > limit
    ignore = (df.groupby(group).filter(lambda grp: ((len(grp)<limit_low) or (len(grp)>limit_high)))).index
    # only ignore rows which are null
    ignore = df.loc[~notnull].index.intersection(ignore)
    keep = df.index.difference(ignore)
    # interpolate only the kept rows
    df.loc[keep] = df.loc[keep].fillna((df.ffill()+df.bfill())/2)
    
    return df


# Extracts a subset of a dataframe between a start and an end date and time.
def get_df_period(df, startdatetime, enddatetime):
    mask = (df.index >= startdatetime) & (df.index <= enddatetime)
    print(f"Getting dataframe between {startdatetime} and {enddatetime}")
    df_period = df[mask]
    return df_period


# Function to obtain a resampled dataframe with a row per second, with NaN values interpolated
def get_df_bysecond_interpolated(df_purchases, startdate, enddate):
    
    # Empty dataframe with a row for each second between two datetimes
    df2 = pd.DataFrame(0, index=pd.date_range(startdate, enddate, freq="1s"), columns=['values']) # temporary empty df where the index are all seconds on that period
    print(df2.shape)
    
    # Extract subset of df_purchases between the start and end dates
    df_subset = df_purchases
    df_by_second = df_subset.combine_first(df2) # adds 
    print(df_by_second.shape)

    # Now resample per second:
    df_by_second = df_by_second.resample("1s").count()
    df_by_second.replace(0, np.nan, inplace=True) # 0 to NaN
    
    # Apply interpolate_rolling_limit()
    df_by_second_interpolated_limit = interpolate_limit(df_by_second, 60, 1500)
    print(df_by_second_interpolated_limit.shape)
    
    return df_by_second_interpolated_limit


# Returns a dataframe matrix of the top 50 users with most purchases during the last week
def get_df_top_users(df_purchases):
    
    print("Getting top users for this week...")
    
    # convert timestamp to datetime (utc)
    df_purchases['timestamp'] = pd.to_datetime(df_purchases['timestamp'], unit='s', origin='unix')
    
    # Convert to Shanghai timezone
    df_purchases['timestamp'] = df_purchases['timestamp'].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)

    # Find the top X users by occurrence
    X = 50
    top_users = df_purchases['user'].value_counts().sort_values(ascending=False).head(X).index.tolist()
    print(top_users)

    # Create an empty dataframe to store the data for the top X users
    df_top_users = pd.DataFrame()

    # Iterate over the top X users
    for user in top_users:
        
        # Select the rows for the current user
        user_df = df_purchases[df_purchases['user'] == user]

        # Group the data by day and count the occurrences
        grouped_df = user_df.groupby(user_df['timestamp']).size().reset_index(name='count')

        # Set the timestamp column as the index of the dataframe
        grouped_df['timestamp'] = pd.to_datetime(grouped_df['timestamp'], infer_datetime_format=True)
        grouped_df.set_index('timestamp', inplace=True)

        # Create a new dataframe with a row for each date
        dates_df = grouped_df.resample('D').count()

        # Fill the missing values with zeros
        dates_df.fillna(0, inplace=True)

        # Append the data for the current user to the top_users_df dataframe as a new column
        df_top_users = pd.concat([df_top_users, dates_df], axis=1)

    # Rename the columns of the df_top_users dataframe using the list of top users
    df_top_users.columns = top_users

    # Fill the missing values with zeros
    df_top_users.fillna(0, inplace=True)
    
    return df_top_users


print("Importing helper functions...")