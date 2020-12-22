# Carbon Citizens 

## ETL / Storage / Modeling and viz pipeline for Carbon Citizens data.

22/12/2020
- **Local_DB_data_insertion.py** grabs the latest csv file from the specified folder, uses Pandas to transform the date and dumps a fresh copy into another specified folder to track transformations in subsequent iterations. 
- It then connects to a specified local PostgreSQL DB and upserts the new data from the CSV, ignoring duplicate rows (which are identified by a composite primary key in the DB)
- Prints a quick statement showing the number of rows copied after deduping, the origin file name, the destination table and the time elapsed.

- **cc_reporting_vizes.ipynb** is a visualiation and report prototyping notebook that pulls the data from the local PostgreSQL DB

Early stages yet, but eventually this will be an automated retrieval, storage, modelling and visualiation pipeline for all of the data available
from Carbon Citizens, including Tunecore, YouTube, Spotify etc. 

The plan is to deploy automated dashboards (most likely with Streamlit) and develop an ML model to aid and influence business and marketing decisions


## Creator

* Michael Taverner
* https://github.com/Tavnuh
* https://www.linkedin.com/in/michael-t-20b27797/

