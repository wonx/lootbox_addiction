import os
import pandas as pd
import numpy as np
import time # For timing executions
import pytz # For managing time zones
import json # Reading / Saving dicts to json
from tqdm import tqdm # To show progress bar when applying lambda function
import argparse # For command-line arguments

# Extract just the new rows in the df_purchases not yet present in df_purchases_value
def get_new_rows(df_purchases, df_purchases_value):
    if df_purchases_value.empty: # If df_purchases_value is empty, all rows in df_purchases are "new"
        return df_purchases
    
    merged_df = pd.merge(df_purchases, df_purchases_value, on=['timestamp', 'user'], how='inner', suffixes=('', '_old'))
    unique_df = df_purchases[~df_purchases.set_index(['timestamp', 'user']).index.isin(merged_df.set_index(['timestamp', 'user']).index)]
    return unique_df


# Parses the new rows into new features (e.g.out_1, out_2..)
def parse_new_values(new_rows):
    # Drop datetime and time columns, they are redundant
    print(f"Are the 'timestamp' and 'time' columns equal? {new_rows['time'].equals(new_rows['timestamp'])}")
    new_rows = new_rows.drop(columns=['datetime', 'time'])
    
    # Convert timestamp to datetimeUTC i datetime_zh
    new_rows['datetimeUTC'] = pd.to_datetime(new_rows['timestamp'], unit='s') # Will be in the UTC timezone by default
    new_rows['datetime_zh'] = new_rows['datetimeUTC'].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    
    # Sort by purchase datetime
    new_rows.sort_values(by='timestamp', inplace=True)
    
    # Parse, 1st stage
    
    # Split the `out` column of the lootbox dataset into three different features
    new_rows[['out_1', 'out_2', 'out_3']] =  new_rows['out'].str.split("|", expand=True)
    
    # Let's separate whatever is within parenthesis as new features
    #the u prefix is to specify that the string is a Unicode string. e.g. 截短霰弹枪（纪念品） 
    new_rows['out_1_par'] = new_rows['out_1'].str.strip().str.extract(r'（(.*?)）')
    new_rows['out_2_par'] = new_rows['out_2'].str.strip().str.extract(r'（([^（）]*)）$')
    
    # Also, let's put the part with no parentheses in another column
    new_rows['out_1_nopar'] = new_rows['out_1'].str.replace(r'（[^（）]*）(?=[^（）]*$)', '', regex=True)
    new_rows['out_2_nopar'] = new_rows['out_2'].str.replace(r'（[^（）]*）(?=[^（）]*$)', '', regex=True)
    
    # out_2_par has commas, so the column can be splitted into more features. 
    #  might fail if all the rows are NaN, so we use a try block to control these cases
    try:
        new_rows[['out_2_par_1', 'out_2_par_2']] = new_rows['out_2_par'].str.split("，", expand=True)
    except ValueError:
        new_rows['out_2_par_1'] = np.nan
        new_rows['out_2_par_2'] = np.nan

    # Rearrange the features and convert the Nan to empty strings
    new_rows = new_rows[['datetime_zh', 'timestamp', 'user', 'src', 'out', 'out_1_nopar', 'out_1_par', 'out_2_nopar', 'out_2_par_1', 'out_2_par_2', 'out_3']]
    new_rows = new_rows.fillna('')
    
    # sort and reset index
    new_rows.sort_values(by='timestamp', inplace=True)
    new_rows.reset_index(drop=True, inplace=True)
    
    return new_rows


# Fills new rows with info from df_src
def get_src_info(new_rows, df_src):
    
    # Load dictionary for manually correcting some missing values
    with open('../lootbox_db_scraping/lootboxes_zh_en.json', 'r', encoding='utf-8') as file:
        lootboxes_zh_en = json.load(file)
    
    # Now map the values of df_src into the df_purchases (merge both dataframes)
    merged_df = new_rows.merge(df_src, left_on='src', right_on='lootbox_zh', how='left')
    new_rows["src_en"] = merged_df["lootbox_en"] # this is not being applied for some reason 
    new_rows['src_en'] = new_rows['src'].map(lootboxes_zh_en).fillna(new_rows['src_en'])
    
    
    # Add the type of lootbox into df_purchases
    merged_df = new_rows.merge(df_src, left_on='src_en', right_on='lootbox_en', how='left')
    new_rows["src_type"] = merged_df["Type_en"]
    
    # Manually add some type of lootbox that wasn't done automatically
    new_rows["src_type"] = np.where((new_rows['src_en'].str.contains('Sticker Capsule')) & (new_rows['src_type'].isnull()), "Sticker Capsules", new_rows["src_type"]) # e.g. 10 Year Birthday Sticker Capsule
    new_rows["src_type"] = np.where((new_rows['src_en'].str.contains('Case')) & (new_rows['src_type'].isnull()), "Cases", new_rows["src_type"]) # e.g.: Falchion case
    new_rows["src_type"] = np.where((new_rows['src_en'].str.contains('Music Kit')) & (new_rows['src_type'].isnull()), "Music Kit Boxes", new_rows["src_type"]) # e.g.: Initiators Music Kit Box

    
    # Add the price to each lootbox (some of them manually)
    new_rows['src_value'] = merged_df["Value"]
    new_rows['src_value'] = np.where((new_rows['src_type'] == 'Music Kit Boxes') | (new_rows['src_type'] == 'Graffiti Capsules'), merged_df['Value'], np.nan)
    new_rows['src_value'] = np.where(((new_rows['src_type'] == 'Cases') & (new_rows['src_value'].isnull())), 2.5, new_rows['src_value']) # Cases
    new_rows['src_value'] = np.where(((new_rows['src_type'] == 'Souvenir Packages') & (new_rows['src_value'].isnull())), 3.0, new_rows['src_value']) # Souvenir packages
    new_rows['src_value'] = np.where(((new_rows['src_type'] == 'Patch Packs') & (new_rows['src_value'].isnull())), 2.0, new_rows['src_value']) # Patch packs
    new_rows['src_value'] = np.where(((new_rows['src_type'] == 'Pins Capsules') & (new_rows['src_value'].isnull())), 9.49, new_rows['src_value']) # Pin capsules
    new_rows['src_value'] = np.where(((new_rows['src_type'] == 'Sticker Capsules') & (new_rows['src_value'].isnull())), 0.95, new_rows['src_value']) # Sticker capsules

    # Convert values like $ 1.21 to 1.21 (float) and convert whole column to float
    new_rows['src_value'] = new_rows['src_value'].apply(lambda x: x.strip('$') if isinstance(x, str) and "$" in x else x)
    new_rows['src_value']= pd.to_numeric(new_rows['src_value'], errors='coerce') # Added errors='coerce' for robustness
    
    # Rearrange column in df_purchases so src_en goes after src
    new_rows = new_rows[['datetime_zh', 'timestamp', 'user', 'src', 'src_en', 'src_type', 'src_value', 'out', 'out_1_nopar',
       'out_1_par', 'out_2_nopar', 'out_2_par_1', 'out_2_par_2', 'out_3']]
    
    # remove temporary variables
    del merged_df
    
    return new_rows


# This functions returns the type and value of `out` for the df_purchases, by checking the df_out dataframe
def get_value(purchase, df_out_local, verbose=False):
    outcategory = ''
    stripped = [s.strip() for s in purchase[8:]]
    purchase = purchase[:8] + stripped
    if verbose: print("Purchase: ", purchase)

    # Some specific cases to deal with manually
    if purchase[8] == 'CZ75': purchase[8] = 'CZ75 自动手枪' 
    if purchase[8] == 'M4A1 消音型': purchase[8] = 'M4A1 消音版' 

    value_item = np.nan # Initialize as NaN (float)

    if purchase[8] == '印花': # If it's a sticker
        if verbose: print("It's a sticker")
        outcategory = "Regular Stickers"
        if verbose: print(f"Name of sticker {purchase[10]}")
        if verbose: print(f"Grade of the sticker: {purchase[11]}")
        if purchase[13] != "": # If there's something in out3, it's a tournament sticker
            outcategory = "Tournament Sticker"
            if verbose: print("It's a tournament sticker")
            if verbose: print(f"It belongs to the tournament {purchase[13]}")
            df_query = df_out_local.query("Weapon_zh == @purchase[10] & Skin_Name_zh == @purchase[13]")
        else:
            if verbose: print("It's a non-tournament sticker")
            df_query = df_out_local.query("Type_zh == '普通贴纸' & Weapon_zh == @purchase[10] & Skin_Name_zh == @purchase[13]")

        value_from_query =  df_query['Value']
        if not value_from_query.empty:
            value_item = value_from_query.iloc[0]

    elif purchase[8] == '音乐盒':
        if verbose: print("It's a music kit")
        df_query = df_out_local.query("Type_en == 'Music Kits' & Weapon_zh == @purchase[8]")
        outcategory = "Music Kits"
        value_from_query =  df_query['Value']
        if not value_from_query.empty:
            value_item = value_from_query.iloc[0]

    elif '★' in purchase[9]:
        if verbose: print("Item with a star! ★")
        searchitem = purchase[8]+'（'+purchase[9]+'）'
        if verbose: print(searchitem)
        df_query = df_out_local.query("Weapon_zh == @searchitem & Skin_Name_zh == @purchase[10]")
        value_from_query =  df_query['Value']
        if not value_from_query.empty:
            value_item = value_from_query.iloc[0]

    else:
        if verbose: print("It's likely a weapon skin") 
        df_query = df_out_local.query("Skin_Name_zh == @purchase[10] & Weapon_zh == @purchase[8] ")
        outcategory = "Unknown Weapon skin"
        value_from_query = pd.Series() # Initialize as empty Series

        if purchase[9] == '纪念品':
            if verbose: print("It's a Souvenir weapon.")
            value_from_query =  df_query['Value_Souvenir']
        elif purchase[9] == 'StatTrak™':
            if verbose: print("It's a StatTrak weapon.")
            value_from_query =  df_query['Value_Stattrak']
        elif purchase[9] == '':
            if verbose: print("The weapon has the grade Normal.")
            value_from_query =  df_query['Value']

        if not value_from_query.empty:
            value_item = value_from_query.iloc[0]
        else:
            if verbose: print("What is this?") # if anything else fails
            return 'not found', np.nan # Ensure np.nan is returned here

    # Parse value (will be in '$ 34 - $ 56' format)
    if not pd.isna(value_item): # Only process if it's not already NaN
        if '-' in str(value_item): value_item = str(value_item).split(' - ')[0] 
        value_item = str(value_item).replace('$', '').strip() 
        try:
            value_item = float(value_item) # Crucially convert to float
        except ValueError:
            value_item = np.nan # If conversion fails, it's NaN

    final_out_type = df_query.iloc[0]['Type_en'] if not df_query.empty and 'Type_en' in df_query.columns else outcategory
    return final_out_type, value_item # Always return a float or np.nan for value_item


def get_out_info(new_rows, df_out_global):
    tqdm.pandas()
    df_purchases_value_temp = new_rows.copy()
    df_purchases_value_temp[['out_type', 'out_value']] = df_purchases_value_temp.progress_apply(lambda row: pd.Series(get_value(list(row), df_out_global, verbose=False)), axis=1)
    return df_purchases_value_temp

def process_new_rows(initial_rows_limit=None):
    
    print("Importing dataframes...")
    df_purchases_path = '../processed_dataframes/df_purchases.parquet'
    df_purchases_value_path = '../processed_dataframes/df_purchases_value.parquet'
    df_src_path = '../lootbox_db_scraping/df_pickles/df_src.parquet'
    df_out_path = '../lootbox_db_scraping/df_pickles/df_out.parquet'

    df_purchases = pd.read_parquet(df_purchases_path)
    
    is_initial_run = False
    if os.path.exists(df_purchases_value_path):
        df_purchases_value = pd.read_parquet(df_purchases_value_path)
        print("df_purchases_value.parquet loaded for appending.")
    else:
        # Define the expected columns for df_purchases_value
        expected_columns = ['datetime_zh', 'timestamp', 'user', 'src', 'src_en', 'src_type', 'src_value', 
                            'out', 'out_1_nopar', 'out_1_par', 'out_2_nopar', 'out_2_par_1', 'out_2_par_2', 'out_3',
                            'out_type', 'out_value']
        df_purchases_value = pd.DataFrame(columns=expected_columns).astype({'out_value': float, 'src_value': float}) # Explicitly set numeric types for new empty DF
        print("df_purchases_value.parquet does not exist, initializing empty DataFrame.")
        is_initial_run = True

    if is_initial_run and initial_rows_limit is not None and initial_rows_limit > 0:
        print(f"Limiting initial processing to the last {initial_rows_limit} rows of df_purchases for testing.")
        df_purchases = df_purchases.tail(initial_rows_limit)

    df_src = pd.read_parquet(df_src_path)
    df_out = pd.read_parquet(df_out_path)
    
    print(f"\nExisting df_purchases_value shape: {df_purchases_value.shape}")
    print("Extracting new rows from df_purchases")
    st = time.time()
    df_purchases_new = get_new_rows(df_purchases, df_purchases_value)
    print(f"New rows to process: {df_purchases_new.shape}")
    et = time.time()
    print('  execution time:', et - st, 'seconds')
    
    if df_purchases_new.empty:
        print("No new lines to process, exiting.")
        return 

    print("\nParsing new rows")
    st = time.time()
    df_purchases_new = parse_new_values(df_purchases_new)
    et = time.time()
    print('  execution time:', et - st, 'seconds')

    print("\nFilling new rows with data from df_src (src_en, src_type and src_value)")
    st = time.time()
    df_purchases_new = get_src_info(df_purchases_new, df_src)
    et = time.time()
    print('  execution time:', et - st, 'seconds')
    
    print("\nFilling new rows with data from df_out")
    st = time.time()
    df_purchases_new = get_out_info(df_purchases_new, df_out)
    et = time.time()
    print('  execution time:', et - st, 'seconds')

    # Manually insert prices to 'out_value' based on 'out' from dictionary
    print("\nAdding missing prices manually from dict")
    manual_prices_path = '../lootbox_db_scraping/manual_prices.json'
    with open(manual_prices_path, 'r', encoding='utf-8') as file:
            manual_prices = json.load(file)
            # Ensure manual prices are floats
            manual_prices = {k: float(v) if isinstance(v, (int, float, str)) and str(v).replace('.', '', 1).isdigit() else np.nan for k, v in manual_prices.items()}

    df_purchases_new['out_value'] = df_purchases_new['out'].map(manual_prices).fillna(df_purchases_new['out_value'])
    
    if not df_purchases_value.empty:
        df_purchases_value['out_value'] = df_purchases_value['out'].map(manual_prices).fillna(df_purchases_value['out_value'])
    else:
        df_purchases_new['out_value'] = df_purchases_new['out'].map(manual_prices).fillna(df_purchases_new['out_value'])

    print("Valid new data %", df_purchases_new['out_value'].count()/df_purchases_new['out'].count()*100)
    if not df_purchases_value.empty:
        print("Valid values old data %", df_purchases_value['out_value'].count()/df_purchases_value['out'].count()*100)
    else:
        print("Valid values old data %: N/A (DataFrame was empty)")
    
    print("\nConcatenating new rows to df_purchases_value")
    # After concat, ensure 'out_value' is consistently numeric
    df_purchases_value = pd.concat([df_purchases_value, df_purchases_new], ignore_index=True)
    df_purchases_value['out_value'] = pd.to_numeric(df_purchases_value['out_value'], errors='coerce') # Force to numeric
    df_purchases_value['src_value'] = pd.to_numeric(df_purchases_value['src_value'], errors='coerce') # Force to numeric
    print(f"Final df_purchases_value shape: {df_purchases_value.shape}")
    
    df_purchases_value.sort_values(by='timestamp', inplace=True)
    df_purchases_value.reset_index(drop=True, inplace=True)
          
    df_purchases_value = df_purchases_value.drop_duplicates(
                              subset = ['timestamp', 'user'],
                              keep = 'last').reset_index(drop = True)
          
    print("\nSaving df_purchases_value to parquet")
    df_purchases_value.to_parquet(df_purchases_value_path)
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate or append to df_purchases_value.parquet.")
    parser.add_argument('--initial-rows', type=int, help='Limit the number of rows processed from df_purchases on the initial run (when df_purchases_value.parquet does not exist).')
    args = parser.parse_args()

    lockfile_path = "../processed_dataframes/df_purchases_value.lock"
    
    if os.path.isfile(lockfile_path):
        print("Lockfile exists, exiting...")
    else:
        with open(lockfile_path, "w") as lockfile:
            lockfile.write("locked")
            
        process_new_rows(initial_rows_limit=args.initial_rows)
        
        print("Removing lockfile...")
        try:
            os.remove(lockfile_path)
        except OSError as e:
            print(f"Failed to remove lockfile: {e.strerror} (Error code: {e.errno})")
        
        print("All done!")