# GameGuard under the hood

## Workflow diagram
![](https://github.com/wonx/lootbox_addiction/blob/main/docs/workflow.png?raw=true)

## File dependency diagram
![](https://github.com/wonx/lootbox_addiction/blob/main/docs/gameguard_file_relationship.png?raw=true)

## Data ingestion

### Lootbox purchase data

`getlootboxjson.py`

    input:
        JSON stream (https://www.csgo.com.cn/api/lotteryHistory)
    output:
        data_ingestion/csv/yyyy-mm-dd_lootboxpurchases.csv

`generate_df_purchases.py`

    input:
        data_ingestion/csv/yyyy-mm-dd_lootboxpurchases.csv
    output:
        processed_dataframes/df_purchases.pkl

### Lootbox price data
`lootbox_db_scraping/lootbox_db_scraping.ipynb`

    input:
        https://wiki.cs.money
        lootbox_db_scraping/lootboxes_zh_en.json
        lootbox_db_scraping/skins_en_zh.json
        lootbox_db_scraping/manual_prices.json

    output:
        lootbox_db_sraping/df_pickles/df_src.pkl
        lootbox_db_sraping/df_pickles/df_out.pkl


### Gambling data

ML model training:
    `gambling_dataset/ML_model/training_ML_model.ipynb`

    input:
        gambling_dataset/AnalyticDataset_Gray_LaPlante_PAB_2012.dat
    output:
        randomforestclassifier_gambling.pkl




## Data cleaning and processing
`generate_df_purchases.py`

    input:
        data_ingestion/csv/yyyy-mm-dd_lootboxpurchases.csv
    output:
        processed_dataframes/df_purchases.pkl


## Data harmonization
`variable_harmonization.py`

    input:
        processed_dataframes/df_purchases_value.pkl

### Daily aggregates
    output:
        processed_dataframes/df_purchases_dailyaggregate.pkl

### Analytic dataset
    output:
        processed_dataframes/df_purchases_analytic.pkl
        processed_dataframes/df_purchases_analytic_weekly/yyyy-mm-dd-df_purchases_analytic.pkl

## Machine Learning model training
`gambling_dataset/ML_model/training_ML_model.ipynb`

    input:
        gambling_dataset/AnalyticDataset_Gray_LaPlante_PAB_2012.dat
    output:
        gambling_dataset/ML_model/randomforestclassifier_gambling.pkl


## ML Predictions
`ML_addiction_predictions.py`

    input:
        gambling_dataset/ML_model/randomforestclassifier_gambling.pkl
        processed_dataframes/df_purchases_analytic.pkl
        processed_dataframes/df_purchases_analytic_weekly/yyyy-mm-dd-df_purchases_analytic.pkl
    output:
        processed_dataframes/df_purchases_analytic_predictions_date.pkl
        processed_dataframes/df_purchases_analytic_predictions.pkl


## Data visualization

### Flask app
`flask_app/app.py`
#### Main dashboard
`/` 

    input:
        processed_dataframes/df_purchases.pkl
        processed_dataframes/df_purchases_dailyaggregate.pkl
        processed_dataframes/df_purchases_analytic_predictions.pkl
#### User profile
`/user/{user}`

    input:
        processed_dataframes/df_purchases_dailyaggregate.pkl
        processed_dataframes/df_purchases_analytic_predictions.pkl
        processed_dataframes/df_purchases_analytic_predictions_date.pkl
#### Raw data
`/user/{user}/{date}`

    input:
        processed_dataframes/df_purchases_value.pkl

## Setting things in motion
### Launcher script
`launch.py`

Schedule:
- `screen -dmS 'lootbox_flask' python3 app.py` at startup
- `getlootboxjson.py` every 2 minutes
- `generate_df_purchases.py` every 10 minutes
- `generate_df_purchases_value.py` every 30 minutes
- `variable_harmonization.py` every day at 16:02 UTC
- `ML_addiction_predictions.py` every monday at 16:15 UTC