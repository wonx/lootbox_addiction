import os
import subprocess
from datetime import datetime, time
from apscheduler.schedulers.blocking import BlockingScheduler
import tzlocal
import subprocess

base_dir = os.getcwd()


def run_data_ingestion():
    subprocess.run(['python', 'getlootboxjson.py'], cwd='data_ingestion')
    print_jobs()

    
def run_generate_df_purchases():
    subprocess.call(['python', 'generate_df_purchases.py',  '--append-new-data'], cwd='data_ingestion')
    print_jobs()

    
def run_generate_df_purchases_value():
    subprocess.call(['python', 'generate_df_purchases_value.py'], cwd='data_ingestion')
    print_jobs()


def run_variable_harmonization():
    subprocess.call(['python', 'variable_harmonization.py'], cwd='data_processing')
    print_jobs()

    
def run_ML_addiction_predictions():
    subprocess.call(['python', 'ML_addiction_predictions.py'], cwd='data_processing')
    print_jobs()
    
    
def run_flask_app():
    global my_subprocess
    my_subprocess = subprocess.Popen(['screen', '-dmS', 'lootbox_flask', 'python3', 'app.py'], cwd='flask_app')
    
def print_jobs():
    print("\n\nActive jobs:")
    for job in scheduler.get_jobs():
        print(job)
    print("\n")

scheduler = BlockingScheduler(timezone=str(tzlocal.get_localzone()))
scheduler.add_job(run_data_ingestion, 'interval', minutes=2, id='data_ingestion', replace_existing=True, max_instances=1)
scheduler.add_job(run_generate_df_purchases, 'interval', minutes=10, id='generate_df_purchases', replace_existing=True, max_instances=1, jitter=60)
scheduler.add_job(run_generate_df_purchases_value, 'interval', minutes=30, id='generate_df_purchases_value', replace_existing=True, max_instances=1, jitter=120)
scheduler.add_job(run_variable_harmonization, 'cron', day_of_week='*', hour=16, minute=2, id='variable_harmonization', replace_existing=True, max_instances=1, jitter=120)
scheduler.add_job(run_ML_addiction_predictions, 'cron', day_of_week='mon', hour=16, minute=15, id='ML_addiction_predictions', replace_existing=True, max_instances=1, jitter=120)

# Start the flask app as background process
print("Starting flask app...")
run_flask_app()

print_jobs()

print("Starting scheduled jobs...")
scheduler.start()
