# Data ingestion

## getlootboxjson.py

*getlootboxjson.py* is the main ingestion script. It gathers data for lootbox purchases for the first person shooter videogame Counter Strike: Global Offensive for the Chinese market available through an API. 

It monitors new data from the JSON stream at https://www.csgo.com.cn/api/lotteryHistory and saves to csv files in the `csv` subdir. It is thought to be run at periodic intervals as a background process. Since the JSON stream is updated every 10 minutes, anything less than that interval will work. It will automatically take care of duplicated entries.

For instance, it can be set to run every two minutes using:

`screen -dmS "lootboxes_date" watch -n120 '/usr/bin/python3 getlootboxjson.py'` 


It will use the timestamp field in the JSON data to place the records in daily files, starting by the date in yyyy-mm-dd format (e.g. 2023-02-11_lootboxpurchases.csv). Additionally, it adds a column to the csv, datetime, with the date and time of the purchase, in the UTC timezone.

The general format of the csv files will be like this:

```
datetime,timestamp,user,src,out,time
2023-02-11 23:40:41,1676158841,S5***-MUNE,命悬一线武器箱,UMP-45 | 白狼,1676158841
2023-02-11 23:40:43,1676158843,AN***-PAYG,Espionage Sticker Capsule,印花 | Zap Cat,1676158843
2023-02-11 23:40:46,1676158846,AH***-TAUL,梦魇武器箱,P2000 | 升天,1676158846
2023-02-11 23:40:47,1676158847,S5***-MUNE,命悬一线武器箱,MP9（StatTrak™） | 黑砂,1676158847
2023-02-11 23:40:48,1676158848,SB***-8PSC,2022年里约热内卢锦标赛挑战组印花胶囊,印花 | BIG | 2022年里约热内卢锦标赛,1676158848
2023-02-11 23:40:48,1676158848,SG***-S8GL,命悬一线武器箱,XM1014 | 锈蚀烈焰,1676158848
2023-02-11 23:40:48,1676158848,AC***-E2YA,梦魇武器箱,PP-野牛 | 太空猫,1676158848
```

## generate_df_purchases.py

*generate_df_purchases.py* is the main tool responsible for generating the pickle files upon which the app relies. It is able to generate the `df_purchases` dataframe from scratch from all the individual `.csv` files in `.csv/` subdir, as a one time thing. Its main purpose, however, is to be ran periodically (e.g. every 10 minutes) to update that dataframe with the new information that has been added to the newest daily csv. By default, it will place the df_purchases.pkl file inside `./processed_dataframes/`. Run `python generate_df_purchases.py --help` to see the available parameters.

For instance, it can be set to run every 10 minutes using:

`screen -dmS "lootbox_generate_df_purchases" watch -n600 '/usr/bin/python3 generate_df_purchases.py --append-new-data'` 


## split_csv_bydate.py

The file *split_csv_bydate.py* is a tool to split a large csv file containing records comprising many days into individual daily csv files by based on the UNIX timestamp present in the second column. It is not part of the general workflow of the program, it's just an auxiliary tool to convert old csv files into the current daily file structure. By default it is hardcoded to look for the *output.csv* file and will generate a series of csv files inside the `split/` directory.

For instance, it will turn:

```output.csv```

into:
 
```
2023-01-30_lootboxpurchases.csv
2023-01-31_lootboxpurchases.csv
2023-02-01_lootboxpurchases.csv
2023-02-02_lootboxpurchases.csv
2023-02-03_lootboxpurchases.csv
2023-02-04_lootboxpurchases.csv
2023-02-05_lootboxpurchases.csv
2023-02-06_lootboxpurchases.csv
2023-02-07_lootboxpurchases.csv
2023-02-08_lootboxpurchases.csv
2023-02-09_lootboxpurchases.csv
2023-02-10_lootboxpurchases.csv
2023-02-11_lootboxpurchases.csv
2023-02-12_lootboxpurchases.csv
```

## mergecsv.py

Another auxiliary tool. *mergecsv.py* does exactly the opposite of `split_csv_bydate.py`, it merges several csv files into a single `merged.csv`.
It's actually not being used (see `generate_df_purchases.py` for its replacement), but still kept here for historical reasons. 
