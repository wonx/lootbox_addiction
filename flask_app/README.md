# Flask app

This is the main deliverable of the whole lootbox addiction project. It is a
flask app that exposes a website containing a dashboard showing stats for
lootbox purchases, users in risk of addiction and the ability to browse the
raw data per user and date.

It relies on several dataframes in *pickle* (.pkl) format as data sources.
For instance `df_purchases_value.pkl`, `df_purchases_dailyaggregate.pkl`, `df_purchases_analytic.pkl` and `df_purchases_analytic_predictions.pkl`.

The app can be launched by running the command:

`python3 app.py`

And it will be available at http://localhost:5000 by default.
