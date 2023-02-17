import os
import pandas as pd
import numpy as np
import time # For timing executions
import pytz # For managing time zones
import json # Reading / Saving dicts to json
from tqdm import tqdm # To show progress bar when applying lambda function


# Extract just the new rows in the df_purchases not yet present in df_purchases_value
def get_new_rows(df_purchases, df_purchases_value):
    merged_df = pd.merge(df_purchases, df_purchases_value, on=['timestamp', 'user'], how='inner')
    unique_df = df_purchases[~df_purchases[['timestamp', 'user']].apply(tuple, axis=1).isin(merged_df[['timestamp', 'user']].apply(tuple, axis=1))]
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
    new_rows['out_1_par'] = new_rows['out_1'].str.strip().str.extract(u'（(.*?)）')
    new_rows['out_2_par'] = new_rows['out_2'].str.strip().str.extract(r'（([^（）]*)）$')
    
    # Also, let's put the part with no parentheses in another column
    new_rows['out_1_nopar'] = new_rows['out_1'].str.replace(r'（[^（）]*）(?=[^（）]*$)', '', regex=True)
    new_rows['out_2_nopar'] = new_rows['out_2'].str.replace(r'（[^（）]*）(?=[^（）]*$)', '', regex=True)
    
    # out_2_par has commas, so the column can be splitted into more features
    # print(new_rows.head(5))
    new_rows['out_2_par'] = new_rows['out_2_par'].fillna('') # If all the rows contain out_2_par as Nan, it fails, so convert to empty string
    new_rows[['out_2_par_1', 'out_2_par_2']] =  new_rows['out_2_par'].str.split("，", expand=True)
    
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
    # Generate dict, but opposite direction (English->Chinese)
    lootboxes_en_zh = {v: k for k, v in lootboxes_zh_en.items()}
    
    # Now map the values of df_src into the df_purchases (merge both dataframes)
    merged_df = new_rows.merge(df_src, left_on='src', right_on='lootbox_zh', how='left')
    new_rows["src_en"] = merged_df["lootbox_en"] # this is not being applied for some reason 
    new_rows['src_en'] = new_rows['src'].map(lootboxes_zh_en).fillna(new_rows['src_en']) # Manually fill missing values from the dict. is this still necessary?
    
    
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
    new_rows['src_value']= pd.to_numeric(new_rows['src_value'])
    
    # Rearrange column in df_purchases so src_en goes after src
    new_rows = new_rows[['datetime_zh', 'timestamp', 'user', 'src', 'src_en', 'src_type', 'src_value', 'out', 'out_1_nopar',
       'out_1_par', 'out_2_nopar', 'out_2_par_1', 'out_2_par_2', 'out_3']]
    
    # remove temporary variables
    del merged_df
    
    return new_rows


# This functions returns the type and value of `out` for the df_purchases, by checking the df_out dataframe
# (this function should be rewriten so it can use column names instead of indices...)
def get_value(purchase, verbose=False):
    outcategory = ''
    stripped = [s.strip() for s in purchase[8:]]
    purchase = purchase[:8] + stripped
    if verbose: print("Purchase: ", purchase)

    # Some specific cases to deal with manually
    if purchase[8] == 'CZ75': purchase[8] = 'CZ75 自动手枪' # The pistol CZ75 appears as CZ75 自动手枪 in df_out. It's an exception that needs to be corrected manually.
    if purchase[8] == 'M4A1 消音型': purchase[8] = 'M4A1 消音版' # The weapon M4A1 消音型 appears as M4A1 消音版 in df_out.

    if purchase[8] == '印花': # If it's a sticker
        # It still fails if a sticker, patch and graffiti have the same name (finds more than 1 result, like item 580721), we have to control those cases
        if verbose: print("It's a sticker")
        outcategory = "Regular Stickers"
        if verbose: print(f"Name of sticker {purchase[10]}")
        if verbose: print(f"Grade of the sticker: {purchase[11]}")
        #if purchase[10] == "冠军": # Champion
        #    print("Champion!")
        if purchase[13] != "": # If there's something in out3, it's a tournament sticker
            outcategory = "Tournament Sticker"
            if verbose: print("It's a tournament sticker")
            if verbose: print(f"It belongs to the tournament {purchase[13]}")
            df_query = df_out.query("Weapon_zh == @purchase[10] & Skin_Name_zh == @purchase[13]")
        else:
            if verbose: print("It's a non-tournament sticker")
            df_query = df_out.query("Type_zh == '普通贴纸' & Weapon_zh == @purchase[10] & Skin_Name_zh == @purchase[13]") # to separate them from graffiti and patches

        value =  df_query['Value']
        if verbose: print(df_query)
    

    elif purchase[8] == '音乐盒':
        if verbose: print("It's a music kit")
        df_query = df_out.query("Type_en == 'Music Kits' & Weapon_zh == @purchase[8]")
        outcategory = "Music Kits"
        value =  df_query['Value']

    elif '★' in purchase[9]:
        if verbose: print("Item with a star! ★")
        searchitem = purchase[8]+'（'+purchase[9]+'）'
        if verbose: print(searchitem)
        df_query = df_out.query("Weapon_zh == @searchitem & Skin_Name_zh == @purchase[10]")
        value =  df_query['Value']
        #print(df_query)

    else:
        if verbose: print("It's likely a weapon skin") # Still fails for music boxes
        #df_weaponsearch = df_out[df_out['Weapon_zh'] == purchase[6]]
        
        df_query = df_out.query("Skin_Name_zh == @purchase[10] & Weapon_zh == @purchase[8] ") # Control if 0 cases are returned, like item with timestamp 1673442333
        outcategory = "Unknown Weapon skin"
        if purchase[9] == '纪念品':
            if verbose: print("It's a Souvenir weapon.")
            value =  df_query['Value_Souvenir']
        elif purchase[9] == 'StatTrak™':
            if verbose: print("It's a StatTrak weapon.")
            value =  df_query['Value_Stattrak']
        elif purchase[9] == '':
            if verbose: print("The weapon has the grade Normal.")
            value =  df_query['Value']

        else:
            if verbose: print("What is this?") # if anything else fails
            return 'not found', np.nan
        if verbose: print(df_query)

    # Parse value (will be in '$ 34 - $ 56' format)
    if verbose: print("value: ", value)
    
    if len(value.index) == 0: # If no results were found, set the value as np.nan
        if verbose: print("No results found")
        if outcategory == "":
            return 'unknown', np.nan 
        else:
            return outcategory, np.nan # sometimes we know the type, even if it was not found
    elif len(value.index) > 1: 
        value = value.head(1) # If more than 1 value is returned, keep the first one. Some rows are repeated in df_out, it's fine
        
    value = value.item()

    if '-' in value: value = value.split(' - ')[0] # If it's a range, get the first value (the lowest)
    if verbose: print("value: ", value)
    value = value.replace('$', '').strip() # remove the $ sign
    if verbose: print("value without $: ", value)
    return df_query.iloc[0]['Type_en'], value # Returns a 2-element tuple with the type of out and its value



def get_out_info(new_rows):
    
    tqdm.pandas() # to show a progress bar. Might make it a bit slower.
    
    #df_purchases_value = df_purchases.iloc[-1000:] # for processing just a slice
    df_purchases_value = new_rows.copy()
    df_purchases_value[['out_type', 'out_value']] = df_purchases_value.progress_apply(lambda row: pd.Series(get_value(list(row), verbose=False)), axis=1)
    #df_purchases_value[['out_type', 'out_value']] = df_purchases_value.apply(lambda row: pd.Series(get_value(list(row), verbose=False)), axis=1) # Non-progress bar option
    
    return df_purchases_value

def process_new_rows():
    
    print("Importing dataframes...")
    df_purchases = pd.read_pickle('../processed_dataframes/df_purchases.pkl')
    df_purchases_value = pd.read_pickle('../processed_dataframes/df_purchases_value.pkl')
    global df_src
    global df_out
    df_src = pd.read_pickle('../lootbox_db_scraping/df_pickles/df_src.pkl')
    df_out = pd.read_pickle('../lootbox_db_scraping/df_pickles/df_out.pkl')

    print(df_purchases.info())
    print(df_purchases_value.info())


    print("\nExtracting new rows from df_purchases")
    st = time.time()
    df_purchases_new = get_new_rows(df_purchases, df_purchases_value)
    print(df_purchases_new.shape)
    et = time.time()
    print('  execution time:', et - st, 'seconds')
    
    print("\nParsing new rows")
    if len(df_purchases_new) != 0:
        st = time.time()
        print(df_purchases_new.head(5))
        df_purchases_new = parse_new_values(df_purchases_new)
        print(df_purchases_new.head(5))
        et = time.time()
        print('  execution time:', et - st, 'seconds')
    else:
        # Nothing to do, remove lock and quit
        print("No new lines to process, exiting.")
        return

    print("\nFilling new rows with data from df_src (src_en, src_type and src_value)")
    st = time.time()
    df_purchases_new = get_src_info(df_purchases_new, df_src)
    #print(df_purchases_new.sample(2))
    et = time.time()
    print('  execution time:', et - st, 'seconds')
    
    print("\nFilling new rows with data from df_out")
    print(df_purchases_new.info())
    st = time.time()
    df_purchases_new = get_out_info(df_purchases_new)
    et = time.time()
    #print(df_purchases_new.sample(3))
    print('  execution time:', et - st, 'seconds')

    # Manually insert prices to 'out_value' based on 'out' from dictionary
    print("\nAdding missing prices manually from dict")
    with open('../lootbox_db_scraping/manual_prices.json', 'r', encoding='utf-8') as file:
            manual_prices = json.load(file)
    df_purchases_new['out_value'] = df_purchases_new['out'].map(manual_prices).fillna(df_purchases_new['out_value'])
    df_purchases_value['out_value'] = df_purchases_value['out'].map(manual_prices).fillna(df_purchases_value['out_value'])
    
    # Check how many prices are missing in the new (and old) data
    print("Valid new data %", df_purchases_new['out_value'].count()/df_purchases_new['out'].count()*100)
    print("Valid values old data %", df_purchases_value['out_value'].count()/df_purchases_value['out'].count()*100)
    
    # Finally, concat new and old values into df_purchases_value
    print("\nConcatenating new rows to df_purchases_value")
    df_purchases_value = pd.concat([df_purchases_value, df_purchases_new])
    print(df_purchases_value.shape)
    
    # Sort & Reset index
    df_purchases_value.sort_values(by='timestamp', inplace=True)
    df_purchases_value.reset_index(drop=True, inplace=True)
          
    # Drop duplicates
    df_purchases_value = df_purchases_value.drop_duplicates(
                              subset = ['timestamp', 'user'],
                              keep = 'last').reset_index(drop = True)
          
    print("\nSaving df_purchases_value to pickle")
    df_purchases_value.to_pickle('../processed_dataframes/df_purchases_value.pkl')
    
    print("All done!")
    
    
if __name__ == '__main__':
    
    lockfile_path = "../processed_dataframes/df_purchases_value.lock"
    
    if os.path.isfile(lockfile_path):
        print("Lockfile exists, exiting...")
    else:
        # Create lockfile
        with open(lockfile_path, "w") as lockfile:
            lockfile.write("locked")
            
        # Processing rows
        process_new_rows()
        
        # Remove lockfile
        os.remove(lockfile_path)