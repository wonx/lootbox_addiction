## Lootbox addiction predictions

# Applying the model trained with the gambling data to predict the risk score of addiction to lootboxes.

# This should be the final step in the data processing. We already have the trained random forest classifier model from the gambling data `randomforestclassifier_gambling.pkl`, and the analytic dataset for lootbox purchase data `df_purchases_analytic.parquet` (or its weekly equivalent).


# This script is thought to be ran once a week, mondays at 00:01 so it can generate the predictions for the last version of df_purchases_analytic.parquet

import pandas as pd
import numpy as np
import pickle
import os

from sklearn.model_selection import GridSearchCV 
from sklearn.ensemble import RandomForestClassifier


# Get the list of dates for the files in the weeksly analytic dataframes
def get_dates():
    dir_path = '../processed_dataframes/df_purchases_analytic_weekly'
    if not os.path.exists(dir_path):
        print(f"Warning: Directory {dir_path} does not exist for weekly analytic data.")
        return [] # Return empty list if directory doesn't exist
        
    file_list = os.listdir(dir_path)

    date_list = []
    for file_name in file_list:
        if file_name.endswith('.parquet'): # <-- FIX: Look for .parquet files
            date_str = file_name[:10]  # extract first 10 characters (yyyy-mm-dd)
            date_list.append(date_str)
    date_list.sort()
    return date_list


# Returns a df with the predictions for each user until a given date
def get_predictions_date(datelimit):
    
    # Initialize an empty DataFrame with expected columns for consistency
    # This prevents 'KeyError' if the weekly file is empty
    expected_cols = ['user', 'addiction', 'confidence_score', 'date']
    df_predictions_for_date = pd.DataFrame(columns=expected_cols)

    date_list = get_dates()
    if not date_list:
        print(f"No weekly analytic parquet files found up to {datelimit}.")
        return df_predictions_for_date # Return empty dataframe

    date_list = [date for date in date_list if date <= datelimit]
    if not date_list:
        print(f"No relevant weekly analytic parquet files found for {datelimit}.")
        return df_predictions_for_date # Return empty dataframe

    date = max(date_list) # the most recent one
    
    print("\nProcessing dataframe for data until", date)
    weekly_file_path = f"../processed_dataframes/df_purchases_analytic_weekly/{date}_df_purchases_analytic.parquet"
    
    if not os.path.exists(weekly_file_path):
        print(f"Warning: Weekly analytic file {weekly_file_path} does not exist.")
        return df_predictions_for_date # Return empty dataframe
        
    df_purchases_analytic = pd.read_parquet(weekly_file_path)
    print(f"Loaded weekly analytic data shape: {df_purchases_analytic.shape}")
    
    if df_purchases_analytic.empty or 'user' not in df_purchases_analytic.columns: # <-- FIX: Check for empty and 'user' column
        print(f"Warning: Weekly analytic dataframe for {date} is empty or missing 'user' column. Skipping predictions.")
        return df_predictions_for_date # Return empty dataframe

    # Drop rows with NaN
    initial_rows = df_purchases_analytic.shape[0]
    df_purchases_analytic.dropna(subset=['sum_stakes_fixedodds', 'net_loss_fixedodds', 'percent_lost_fixedodds'], inplace=True)
    if df_purchases_analytic.shape[0] < initial_rows:
        print(f"Dropped {initial_rows - df_purchases_analytic.shape[0]} rows due to NaN values.")

    if df_purchases_analytic.empty: # After dropping NaNs, it might become empty
        print(f"Warning: Weekly analytic dataframe for {date} became empty after dropping NaNs. Skipping predictions.")
        return df_predictions_for_date # Return empty dataframe

    # Drop 'user' column from df_purchases_analytic before applying the model
    X_test = df_purchases_analytic.drop(columns=['user'])

    # Apply the model to make the predictions based on the lootbox data
    y_pred = rm_classifier.predict(X_test)

    # Store the class probabilities (confidence score)
    probs = rm_classifier.predict_proba(X_test)

    # Add the predicted addiction series (y_pred) to the dataframe
    df_purchases_analytic['addiction'] = y_pred
    df_purchases_analytic['confidence_score'] = probs[:,1]

    print(f"Addicted users in this weekly dataframe: {df_purchases_analytic[df_purchases_analytic['addiction'] == 1].shape}")

    # Store the scores in a temp dataframe
    df_temp = df_purchases_analytic[['user', 'addiction', 'confidence_score']].copy()
    df_temp['date'] = date

    return df_temp


if __name__ == '__main__':
    
    # Load pickles
    print("Loading pickle files...")
    rm_classifier = pickle.load(open('../gambling_dataset/ML_model/randomforestclassifier_gambling.pkl', 'rb')) # The model was trained with scikit-learn==1.0.2 (newer versions might not work)
    
    # --- Check and process df_purchases_analytic.parquet (main analytic file) ---
    main_analytic_path = '../processed_dataframes/df_purchases_analytic.parquet'
    if not os.path.exists(main_analytic_path):
        print(f"Error: Main analytic file {main_analytic_path} does not exist. Please ensure variable_harmonization.py runs successfully first.")
        exit() # Exit if the main analytic file is not found
        
    df_purchases_analytic = pd.read_parquet(main_analytic_path) 
    
    print(f"Main analytic dataframe shape: {df_purchases_analytic.shape}")
    
    if df_purchases_analytic.empty or 'user' not in df_purchases_analytic.columns: # <-- FIX: Check for empty and 'user' column
        print("Warning: Main df_purchases_analytic is empty or missing 'user' column. Skipping predictions for main file.")
        # Create an empty dataframe with expected columns to prevent errors later
        expected_cols = df_purchases_analytic.columns.tolist() + ['addiction', 'confidence_score', 'improving']
        df_purchases_analytic_predictions = pd.DataFrame(columns=expected_cols)
        df_purchases_analytic_predictions['date'] = pd.to_datetime([]) # Ensure date column for later concat
    else:
        # Drop rows with NaN
        initial_rows = df_purchases_analytic.shape[0]
        df_purchases_analytic.dropna(subset=['sum_stakes_fixedodds', 'net_loss_fixedodds', 'percent_lost_fixedodds'], inplace=True)
        if df_purchases_analytic.shape[0] < initial_rows:
            print(f"Dropped {initial_rows - df_purchases_analytic.shape[0]} rows from main analytic due to NaN values.")

        if df_purchases_analytic.empty: # If it becomes empty after dropping NaNs
            print("Warning: Main df_purchases_analytic became empty after dropping NaNs. Skipping predictions for main file.")
            expected_cols = df_purchases_analytic.columns.tolist() + ['addiction', 'confidence_score', 'improving']
            df_purchases_analytic_predictions = pd.DataFrame(columns=expected_cols)
            df_purchases_analytic_predictions['date'] = pd.to_datetime([])
        else:
            print("Making predictions for main analytic file...")
            X_test = df_purchases_analytic.drop(columns=['user'])
            y_pred = rm_classifier.predict(X_test)
            probs = rm_classifier.predict_proba(X_test)
            # Add the predicted addiction series (y_pred) to the main dataframe
            df_purchases_analytic['addiction'] = y_pred
            df_purchases_analytic['confidence_score'] = probs[:,1]
            df_purchases_analytic_predictions = df_purchases_analytic.copy() # Will be used for final saving

    
    print(f"Total valid unique users for main predictions: {len(df_purchases_analytic_predictions) if 'user' in df_purchases_analytic_predictions.columns else 0}")

    
    ### Now let's calculate the risk score for each weekly analytic dataframe
    print("Calculating risk score for each weekly analytic dataframe...")

    date_list = get_dates()
    df_purchases_analytic_predictions_date = pd.DataFrame() # Initialize as empty

    if not date_list: # <-- FIX: Handle empty date_list
        print("No weekly analytic files found to process. df_purchases_analytic_predictions_date will be empty.")
    else:
        for date in date_list:
            df_date = get_predictions_date(date)
            if not df_date.empty: # <-- FIX: Only concat if df_date is not empty
                df_purchases_analytic_predictions_date = pd.concat([df_purchases_analytic_predictions_date, df_date], axis=0, ignore_index=True)

    print(f"Shape of df_purchases_analytic_predictions_date: {df_purchases_analytic_predictions_date.shape}")
    if not df_purchases_analytic_predictions_date.empty:
        print(df_purchases_analytic_predictions_date.tail(6))
    
    # Save to parquet
    print("Saving weekly predictions to parquet (df_purchases_analytic_predictions_date.parquet)")
    df_purchases_analytic_predictions_date.to_parquet('../processed_dataframes/df_purchases_analytic_predictions_date.parquet')


    
    ## Find which users are improving (or worsening) from their addiction
    print("Finding out which users are improving or worsening from their addiction...")

    df = df_purchases_analytic_predictions_date # just to shorten it...

    if df.empty or 'user' not in df.columns or 'addiction' not in df.columns or 'confidence_score' not in df.columns or 'date' not in df.columns: # <-- FIX: Comprehensive check
        print("df_purchases_analytic_predictions_date is empty or missing required columns. Skipping improvement analysis.")
        # Ensure df_purchases_analytic_predictions has 'improving' column even if empty
        if 'improving' not in df_purchases_analytic_predictions.columns:
             df_purchases_analytic_predictions['improving'] = np.nan
        # If the main predictions df is also empty, ensure it has necessary columns for saving
        if df_purchases_analytic_predictions.empty:
            expected_main_cols = ['user', 'sum_stakes_fixedodds', 'sum_bets_fixedodds', 'bettingdays_fixedodds', 
                                  'bets_per_day_fixedodds', 'euros_per_bet_fixedodds', 'net_loss_fixedodds', 
                                  'percent_lost_fixedodds', 'addiction', 'confidence_score', 'improving']
            for col in expected_main_cols:
                if col not in df_purchases_analytic_predictions.columns:
                    df_purchases_analytic_predictions[col] = np.nan
    else:
        # Proceed with analysis only if DataFrame is valid
        grouped = df.groupby('user')
        grouped_addicted = grouped.filter(lambda x: (x['addiction'] == 1).any())

        if grouped_addicted.empty: # <-- FIX: Check if any addicted users exist
            print("No users found with 'addiction' == 1 in historical data. Skipping improvement analysis.")
            # Ensure df_purchases_analytic_predictions still has 'improving' column
            if 'improving' not in df_purchases_analytic_predictions.columns:
                df_purchases_analytic_predictions['improving'] = np.nan
        else:
            addicted_users = grouped_addicted['user'].unique().tolist()
            grouped_addicted = grouped_addicted.sort_values(by=['user', 'date'])

            grouped_addicted['prev_confidence_score'] = grouped_addicted.groupby('user')['confidence_score'].shift() # Use groupby for shift
            
            # Filter for the most recent date for each user
            grouped_addicted_latest = grouped_addicted.loc[grouped_addicted.groupby('user')['date'].idxmax()]

            # Only consider users who have a previous score to compare against
            comparable_users = grouped_addicted_latest.dropna(subset=['prev_confidence_score'])

            if comparable_users.empty:
                print("No users with comparable previous scores found. Skipping detailed improvement analysis.")
            else:
                decreased_users = comparable_users[comparable_users['confidence_score'] < comparable_users['prev_confidence_score']]['user'].unique().tolist()
                equal_users = comparable_users[comparable_users['confidence_score'] == comparable_users['prev_confidence_score']]['user'].unique().tolist()
                increased_users = comparable_users[comparable_users['confidence_score'] > comparable_users['prev_confidence_score']]['user'].unique().tolist()

                print("Improving users:", decreased_users)
                
                ## Add a feature to df_purchases_analytic, indicating if the addiction is getting worse or not
                print("Adding the improving feature to the df_purchases_analytic_predictions...")

                # By default, values are nan (already handled if predictions_df was empty)
                if 'improving' not in df_purchases_analytic_predictions.columns:
                    df_purchases_analytic_predictions['improving'] = np.nan

                # 1 for those users who are improving
                df_purchases_analytic_predictions.loc[df_purchases_analytic_predictions['user'].isin(decreased_users), 'improving'] = 1

                # 0 for those who stay the same
                df_purchases_analytic_predictions.loc[df_purchases_analytic_predictions['user'].isin(equal_users), 'improving'] = 0

                # -1 for those addicted users who are worsening
                df_purchases_analytic_predictions.loc[df_purchases_analytic_predictions['user'].isin(increased_users), 'improving'] = -1
    
    # Finally, save dataframe with predictions to parquet file
    print("Saving df_purchases_analytic_predictions to parquet")
    df_purchases_analytic_predictions.to_parquet('../processed_dataframes/df_purchases_analytic_predictions.parquet')
          
    print("All done!")