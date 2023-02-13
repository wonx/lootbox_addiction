# Microtransaction addiction forecasting through lootbox purchases patterns

This will be the expected pipeline:
Lootbox pipeline

1) Ingestion script (from JSON Stream)

2) Scrape translations and prices to create lootbox DB.

2) Merge csv files
    yyyy-mm-dd_lootboxpurchases.csv
    How often?
        Once a day for the whole data?
        Current day only every 10 minutes?

4) Assign zh_en correspondence and value purchases
    Append missing purchases

5) Process gambling dataset and train model

6) Harmonize variables in lootbox dataset

7) Apply ML model to lootbox dataset


8) Additional
    Interpolated purchases
    Risk score with time (weekly)?
    Detailed user stats
