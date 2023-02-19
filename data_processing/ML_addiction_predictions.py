## Lootbox addiction predictions

# Applying the model trained with the gambling data to predict the risk score of addiction to lootboxes.

# This should be the final step in the data processing. We already have the trained random forest classifier model from the gambling data `randomforestclassifier_gambling.pkl`, and the analytic dataset for lootbox purchase data `df_purchases_analytic.pkl` (or its weekly equivalent).


# This script is thought to be ran once a week, mondays at 00:01 so it can generate the predictions for the last version of df_purchases_analytic.pkl

import pandas as pd
import numpy as np
import pickle
import os

from sklearn.model_selection import GridSearchCV 
from sklearn.ensemble import RandomForestClassifier


# Get the list of dates for the files in the weeksly analytic dataframes
def get_dates():
    dir_path = '../processed_dataframes/df_purchases_analytic_weekly'  # replace with your directory path
    file_list = os.listdir(dir_path)

    date_list = []
    for file_name in file_list:
        if file_name.endswith('.pkl'):
            date_str = file_name[:10]  # extract first 10 characters (yyyy-mm-dd)
            date_list.append(date_str)
    date_list.sort()
    #print(date_list)
    return date_list


# Returns a df with the predictions for each user until a given date
def get_predictions_date(datelimit):
    
    df_predictions = pd.DataFrame(columns=['date', 'user', 'addiction', 'confidence_score'])
    df_temp = pd.DataFrame()
    
    # Get the most recent date from those available
    date_list = get_dates()
    date_list = [date for date in date_list if date <= datelimit]
    date = max(date_list) # the most recent one
    
    print("\nProcessing dataframe for data until", date)
    df_purchases_analytic = pd.read_pickle(f"../processed_dataframes/df_purchases_analytic_weekly/{date}_df_purchases_analytic.pkl") 
    print(df_purchases_analytic.shape)

    # Drop 'user' column from df_purchases_analytic before applying the model
    X_test = df_purchases_analytic.drop(columns=['user'])

    # Apply the model to make the predictions based on the lootbox data
    y_pred = rm_classifier.predict(X_test)

    # Store the class probabilities (confidence score)
    probs = rm_classifier.predict_proba(X_test)

    # Add the predicted addiction series (y_pred) to the dataframe
    df_purchases_analytic['addiction'] = y_pred
    df_purchases_analytic['confidence_score'] = probs[:,1]

    print(df_purchases_analytic[df_purchases_analytic['addiction'] == 1].shape)

    # Store the scores in a temp dataframe
    df_temp[['user', 'addiction', 'confidence_score']] = df_purchases_analytic[['user', 'addiction', 'confidence_score']]
    df_temp['date'] = date

    # Concat the new scores to the new dataframe (df_predictions)
    #df_predictions = pd.concat([df_predictions, df_temp], axis=0, ignore_index=True)

    return df_temp


if __name__ == '__main__':
    
    # Load pickles
    print("Loading pickle files...")
    rm_classifier = pickle.load(open('../gambling_dataset/ML_model/randomforestclassifier_gambling.pkl', 'rb')) # The model was trained with scikit-learn==1.0.2 (newer versions might not work)
    df_purchases_analytic = pd.read_pickle('../processed_dataframes/df_purchases_analytic.pkl') 

    print("Making predictions...")
    # Drop 'user' column from df_purchases_analytic before applying the model
    X_test = df_purchases_analytic.drop(columns=['user'])

    # Apply the model to make the predictions based on the lootbox data
    y_pred = rm_classifier.predict(X_test)

    # Store the class probabilities (confidence score)
    probs = rm_classifier.predict_proba(X_test)
    print(probs[:,1]) 
    print(len(probs[:,1])) # Total valid unique users


    # Add the predicted addiction series (y_pred) to the main dataframe
    df_purchases_analytic['addiction'] = y_pred
    df_purchases_analytic['confidence_score'] = probs[:,1]


    # Save dataframe with predictions to pickle file
    #df_purchases_analytic.to_pickle('../processed_dataframes/df_purchases_analytic_predictions.pkl') # We'll do it later

    
    
    
    ### Now let's calculate the risk score for each weekly analytic dataframe
    print("Calculating risk score for each weekly analytic dataframe...")

    # Generate the predictions for the available dates and concatenate them into a single df
    date_list = get_dates()
    df_purchases_analytic_predictions_date = pd.DataFrame()

    for date in date_list:
        df_date = pd.DataFrame()
        df_date = get_predictions_date(date)
        df_purchases_analytic_predictions_date = pd.concat([df_purchases_analytic_predictions_date, df_date], axis=0, ignore_index=True)

    print(df_purchases_analytic_predictions_date.shape)
    print(df_purchases_analytic_predictions_date.tail(6))
    
    # Save to pickle
    print("Saving weekly predictions to pickle (df_purchases_analytic_predictions_date.pkl)")
    df_purchases_analytic_predictions_date.to_pickle('../processed_dataframes/df_purchases_analytic_predictions_date.pkl')


    
    ## Find which users are improving (or worsening) from their addiction
    
    print("Finding out which users are improving or worsening from their addiction...")
    # Users which at some point were considered addicted
    # group by user and filter for users with addiction
    df = df_purchases_analytic_predictions_date # just to shorten it...
    grouped = df.groupby('user')
    grouped_addicted = grouped.filter(lambda x: (x['addiction'] == 1).any())

    # Create list of addicted users, we'll use it later
    addicted_users = grouped_addicted['user'].unique().tolist()

    # Also sort by date, asc
    grouped_addicted = grouped_addicted.sort_values(by=['user', 'date'])


    # Create a new column for the previous confidence score
    grouped_addicted['prev_confidence_score'] = grouped_addicted['confidence_score'].shift()

    # Only keep the last date
    grouped_addicted = grouped_addicted[grouped_addicted['date'] == max(grouped_addicted['date'])]

    # Filter for users where the confidence score has decreased, stayed the same, or increased
    decreased_users = grouped_addicted[grouped_addicted['confidence_score'] < grouped_addicted['prev_confidence_score']]
    equal_users = grouped_addicted[grouped_addicted['confidence_score'] == grouped_addicted['prev_confidence_score']]
    increased_users = grouped_addicted[grouped_addicted['confidence_score'] > grouped_addicted['prev_confidence_score']]

    # Convert to list of users
    decreased_users = decreased_users['user'].unique().tolist()
    equal_users = equal_users['user'].unique().tolist()
    increased_users = increased_users['user'].unique().tolist()

    print("Improving users:", decreased_users)
    
    
    
    ## Add a feature to df_purchases_analytic, indicating if the addiction is getting worse or not
    print("Adding the improving feature to the df_purchases_analytic_predictions...")

    # By default, values are nan
    df_purchases_analytic['improving'] = np.nan

    # 1 for those users who are improving
    df_purchases_analytic.loc[df_purchases_analytic['user'].isin(decreased_users), 'improving'] = 1

    # 0 for those who stay the same
    df_purchases_analytic.loc[df_purchases_analytic['user'].isin(equal_users), 'improving'] = 0

    # 0 for those addicted users who are worsening
    df_purchases_analytic.loc[df_purchases_analytic['user'].isin(increased_users), 'improving'] = -1
    
    
    # Finally, save dataframe with predictions to pickle file
    print("Saving df_purchases_analytic_predictions to pickle")
    df_purchases_analytic.to_pickle('../processed_dataframes/df_purchases_analytic_predictions.pkl')
          
    print("All done!")