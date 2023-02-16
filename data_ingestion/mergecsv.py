import pandas as pd
import os

# Get a list of all CSV files in the directory
files = [f for f in os.listdir() if f.endswith('.csv')]

# Read the first file into a dataframe
df = pd.read_csv(files[0], encoding='utf-8', encoding_errors='ignore')

# Remove the 'datetime' column
df = df.drop('datetime', axis=1)

# Loop through the remaining files
for i in range(1, len(files)):
    # Read the file into a dataframe
    temp_df = pd.read_csv(files[i], encoding='utf-8')

    # Remove the 'datetime' column
    temp_df = temp_df.drop('datetime', axis=1)



    # Append the unique rows from the file to the result dataframe
    df = df.merge(temp_df, how='outer', on=list(temp_df.columns), suffixes=('', '_'+str(i)))
    df = df.loc[~df.duplicated(), :]

    # Remove rows with NaN values in either the 'timestamp' or 'timestamp_end' columns
    df.dropna(subset=['timestamp', 'time'], inplace=True)

    # Convert the Unix timestamp columns to integers
    df['timestamp'] = df['timestamp'].astype(int)
    df['time'] = df['time'].astype(int)

# Sort the result dataframe by the 'timestamp' column
df = df.sort_values('timestamp')

# Save the result dataframe as a CSV file
df.to_csv('merged.csv', index=False)
