# Dataframe builder

# This script merges data from all the daily purchase csv files into one big pandas dataframe, and saves it as pickle.

# It also monitors new content in the latest csv daily file and periodically adds that data to the main dataframe.

# This script is meant to be run periodically as a background process, as often as you want your data to be up to date

import pandas as pd
import glob
import os
import argparse


def generate_full_dataframe(csv_path='../data_ingestion/csv/', output_file="../processed_dataframes/df_purchases.pkl"):
    print(f"Appending new data from {csv_path} to main dataframe at {output_file}")
    all_files = glob.glob(os.path.join(csv_path , "*.csv"))
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True, axis=0)
    df.to_pickle(output_file)
    print("Full dataframe generated at", output_file) 
    print(df.shape)
    return df
    
def append_new_data(csv_path="../data_ingestion/csv/", output_file="../processed_dataframes/df_purchases.pkl"):
    print(f"Appending new data from {csv_path} to main dataframe at {output_file}")
    #main_df = output_file
    main_df = pd.read_pickle(output_file)
    # Get a list of all the CSV files in the folder
    csv_files = [f for f in os.listdir(csv_path) if f.endswith('.csv')]
    sorted_csv_files = sorted(csv_files, reverse=True)
    most_recent_csv = sorted_csv_files[0]
    print("  Most recent csv: ", most_recent_csv)
    
    print("  Before concatenating:", main_df.shape)
    # Append new info to main dataframe and remove duplicate rows
    main_df = pd.concat([main_df, pd.read_csv(csv_path + most_recent_csv)], ignore_index=True, axis=0)
    main_df.drop_duplicates(inplace=True)
    print("  After concatenating: ", main_df.shape)
    main_df.to_pickle(output_file)
    print("...done")
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=lambda x: parser.print_help())
    
    parser.add_argument('--generate-full-dataframe', action='store_true',
                        help='Generate full dataframe')
    parser.add_argument('--append-new-data', action='store_true',
                        help='Append new data')
    parser.add_argument('--csv_path', type=str, default='../data_ingestion/csv/',
                        help='Output pickle dataframe name with path')
    parser.add_argument('--output_file', type=str, default='../processed_dataframes/df_purchases.pkl',
                        help='Output pickle dataframe name with path')
    
    args = parser.parse_args()
  
    
    if not args.generate_full_dataframe and not args.append_new_data: # show help if no parameters are passed.
        parser.print_help()

    if args.generate_full_dataframe:
        generate_full_dataframe(args.csv_path, args.output_file)
        
    if args.append_new_data:
        append_new_data(args.csv_path, args.output_file)

        
    