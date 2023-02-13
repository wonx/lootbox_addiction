import csv
import requests
import datetime
import subprocess
import pandas as pd

# Path for the CSV files
csv_path = 'csv'

# URL for the JSON file
json_url = "https://www.csgo.com.cn/api/lotteryHistory"

# Fetch the JSON data
print("Fetching JSON metadata")
json_data = requests.get(json_url).json()

# Reverse the order
reversed_records = reversed(json_data["result"])

# set of dates that we dealt with, to later remove duplicates in their respective files
dates = set()

# Loop through the records in the JSON data
for record in reversed_records:
    date_time = datetime.datetime.fromtimestamp(record["timestamp"], tz=datetime.timezone.utc)
    formatted_date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
    date = date_time.strftime("%Y-%m-%d")
    dates.add(date) # add date to set
    filename = f"{csv_path}/{date}_lootboxpurchases.csv"

    # Open the CSV file for appending
    csv_file = open(filename, "a", newline="")

    # Create a CSV writer
    csv_writer = csv.writer(csv_file)

    # Write the record to the CSV file
    csv_writer.writerow([formatted_date_time, record["timestamp"], record["user"], record["src"], record["out"], record["time"]])

    # Close the CSV file
    csv_file.close()


# Remove duplicates in the affected files
print(f"Affected files: {dates}_lootboxpurchases.csv")
for date in dates:
    filename = f"{csv_path}/{date}_lootboxpurchases.csv"
    num_lines = int(subprocess.check_output(['wc', '-l', filename]).decode().split()[0])
    print(f"Lines in {filename} after adding json but before deduplicating: {num_lines}")

    # Read to pandas, deduplicate, and save to csv again
    df = pd.read_csv(filename)
    df.drop_duplicates(inplace=True)
    df.to_csv(filename, index=False)

    num_lines = int(subprocess.check_output(['wc', '-l', filename]).decode().split()[0])
    print(f"Lines in {filename} after deduplicating: {num_lines}")
