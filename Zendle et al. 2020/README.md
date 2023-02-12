# How do loot boxes make money? An analysis of a very large dataset of real Chinese CSGO loot box openings

David Zendle, University of York (david.zendle@york.ac.uk)

Elena Petrovskaya, University of York

Heather Wardle, University of Glasgow


http://dx.doi.org/10.31234/osf.io/5k2sy
https://osf.io/97ves/?view_only=8ac012cedc9c46f582a1b7e3e77d1a6d

The data that was gathered for this study consists of all loot box opening information published by the Chinese CSGO partner PerfectWorld (https://www.csgo.com.cn/hd/1707/lotteryrecords / https://www.csgo.com.cn/api/lotteryHistory) for the 66 days ranging between 2020-07-29 and 2020-10-03.

## Manuscript - Final.pdf
Manuscript itself with the study and the details on the gata ingestion and analysis.

## parsedresults.csv
The file `parsedresults.csv` contains data on 1469913 lootbox openings by 386269 unique users gathered during 66 days. While this data has not been used in the final deliverable, it was used to perform some initial tests on the viability of the project.

## harmonize_parsedresults.ipynb
Small jupyter notebook to harmonize the `parsedresults.csv` dataset to the format I used in this project.

## parsedresults_harmonized.csv
Harmonized dataset.

## parsedresults_daily
Harmonized dataset, converted into daily csv files with the help of the `data_ingestion/split_csv_bydate.py` script.
