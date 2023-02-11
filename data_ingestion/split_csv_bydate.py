
# Separates a unique output.csv file into multiple csv files separated by date (first column of the csv)

import csv
import os
import datetime

# Dictionary to store the data for each date
date_data = {}

# Read the data from the output.csv file
with open('output.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)

    for row in reader:
        date_time = datetime.datetime.fromtimestamp(int(row[1]))
        date_string = date_time.strftime("%Y-%m-%d")

        if date_string in date_data:
            date_data[date_string].append(row)
        else:
            date_data[date_string] = [row]

# Create dir to store the splitted data
if not os.path.exists('split'):
   os.makedirs('split')


# Write the data for each date to a separate file
for date, data in date_data.items():
    filename = f"split/{date}_lootboxpurchases.csv"
    if os.path.exists(filename):
        os.remove(filename)

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)


