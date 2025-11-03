# Sports Analytics Platform

A comprehensive sports analytics platform, starting with NFL, to analyze team and player data - and make predictions about future outcomes.

#### Tech Stack

- **python** - incl. Polars, BeautifulSoup, nflreadpy 
- **dbt** for transformations (implied but, **SQL** too)
- **Google Cloud Warehouse** as a lake, and **BigQuery** as my warehouse
- **terraform** for infrastructure as code (for GCP resources)
- **Apache Airflow** to orchestrate this. (Docker, not using Cloud Composer right now because $$)
- ***more to come as the project continues***

# The Project - What do we have so far? 

**NFL Data Pipeline**
The first part of our p0 project is to implement a data pipeline and get all of our data into a warehouse for use later. Below I'll give an overview of the pipeline design as it stands now. 

#### Data Sources
Starting with where we're getting data from! Right now, there are two primary sources: 
1. nflreadpy - (Link)[https://nflreadpy.nflverse.com/] - this is an insanely useful data source that we are able to get detailed data on games, players, drafts, combines, etc. 

2. NFL.com Gameday Accountability Center - (Link)[https://operations.nfl.com/inside-football-ops/rules-enforcement/gameday-accountability/] - this is where the NFL posts data about player fines applied for each week of the season. It contains the player details as well as the details related to the fine reason and fine amount ($). 

The two sources above are the core required data providers for p0. There are a handful of data sources we have planned, but not yet implemented - these will be added to our pipelines in the coming weeks.

#### Extracting Data - high level
I'm exctracting data using two methods right now, as each source is different. 

**nflreadpy** I'm using python to call the nflreadpy package (/ingestion/nfl/extractor.py & scripts/nfl_data_ingest.py). The extractor file references a data_type parameter (data_type_map), and maps each data type to the nflreadpy function. For example, pbp maps to nfl.load_pbp. It returns the data as a Polars DataFrame. nflreadpy natively uses Polars, and I've decided to keep that and maintain the use of Polars throughout this project - partially because I want to learn Polars after being a Pandas user for a long time! 

**NFL Fines** To get data about fines & gameday accountability, I wrote a script (/ingestion/scrapers/nfl_fines_scraper.py) that scrapes the (NFL Gameday Accountability)[https://operations.nfl.com/inside-football-ops/rules-enforcement/gameday-accountability/] page. The source data is relatively straight forward, with the main complication being the nesting of tables. 

#### Uploading to Google Cloud Storage (GCS)
After extracting data from nflreadpy and the NFL website, I am uploading it in raw format to Google Cloud Storage. my core game / player / team data from nflreadpy is going up as a parquet and the scraped data from the NFL website is going up as JSON. A note: The scraper returns in JSON, but I'm immediately converting this to ndjson. I want to be able to get this to BigQuery - and BQ requires ndjson (not JSON). 

#### Moving to BigQuery
using ingestion/storage/gcs_to_bq_loader.py, I'm moving raw data from GCS to BigQuery so that I can begin transforming. For the NFL.com ndjson data, I've created an external table in BigQuery that references GCS. 

#### dbt models
OK! After getting the data into BigQuery, it's still in a relatively raw state. Using dbt to manage the transformations & model, I start with staging models - which contain light transformations (e.g., naming is the big one). I've kept a 1:1 raw -> stg mapping. I debated whether or not to introduce intermediate tables, but ultimately decided against it for this usecase. In a professional setting I might consider it to improve data quality & access. Right now, I've got a few marts created - but have a list of additional marts to create based on the vast amount of data I've been able pull in here. 

### What's Next? 
Now that we have the core pipeline up & running, I'll move on to implementing my models + an analytics dashboard. This will get us to p0. 

For p1 - I'll continue to add data sources, implement additional predictive modeling, and at some point train an LLM on this for conversational analytics. 

### Questions + Thoughts
**Couldn't you have used cron + other simpler services** - Yes! But, I like to use these tools & services to continue learning - especially for the products that I don't get to use in my day job (i.e., the non-AWS stuff). 

### Things to fix & improve
- [ ] Testing 

- [ ] Right now, loading with a replace functionality. Update this to just look for new records and append. 

- [ ] Injuries keeps breaking. Removed for now, will investigate and re-add. Working hypothesis is that the Seasons param is the root cause. 

- [ ] Rename / reorg BQ datasets 

- [ ] Mage looks cool?

