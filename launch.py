import os
import subprocess
from datetime import datetime, time
from apscheduler.schedulers.blocking import BlockingScheduler
import tzlocal
import subprocess

base_dir = os.getcwd()


def run_data_ingestion():
    os.chdir('data_ingestion')
    subprocess.call(['python', 'getlootboxjson.py'])
    os.chdir(base_dir)
    print_jobs()

    
def run_generate_df_purchases():
    os.chdir('data_ingestion')
    subprocess.call(['python', 'generate_df_purchases.py --append-new-data'])
    os.chdir(base_dir)
    print_jobs()

    
def run_generate_df_purchases_value():
    os.chdir('data_ingestion')
    subprocess.call(['python', 'generate_df_purchases_value.py'])
    os.chdir(base_dir)
    print_jobs()


def run_variable_harmonization():
    os.chdir('data_processing')
    subprocess.call(['python', 'variable_harmonization.py'])
    os.chdir(base_dir)
    print_jobs()

    
def run_ML_addiction_predictions():
    # set the current working directory to the folder containing the script
    os.chdir('data_processing')
    subprocess.call(['python', 'ML_addiction_predictions.py'])
    os.chdir(base_dir)
    print_jobs()
    
    
def run_flask_app():
    global my_subprocess
    os.chdir('flask_app')
    my_subprocess = subprocess.Popen(['screen', '-dmS', 'lootbox_flask', 'python3', 'app.py'])
    os.chdir(base_dir)

    
def print_jobs():
    print("\n\nActive jobs:")
    for job in scheduler.get_jobs():
        print(job)
    print("\n")

scheduler = BlockingScheduler(timezone=str(tzlocal.get_localzone()))
scheduler.add_job(run_data_ingestion, 'interval', minutes=1, id='data_ingestion', replace_existing=True)
scheduler.add_job(run_generate_df_purchases, 'interval', minutes=10, id='generate_df_purchases', replace_existing=True)
scheduler.add_job(run_generate_df_purchases_value, 'interval', minutes=30, id='generate_df_purchases_value', replace_existing=True)
scheduler.add_job(run_variable_harmonization, 'cron', day_of_week='*', hour=0, minute=2, id='variable_harmonization', replace_existing=True)
scheduler.add_job(run_ML_addiction_predictions, 'cron', day_of_week='mon', hour=0, minute=15, id='ML_addiction_predictions', replace_existing=True)

# Start the flask app as background process
print("Starting flask app...")
run_flask_app()

print_jobs()

print("Starting scheduled jobs...")
scheduler.start()
