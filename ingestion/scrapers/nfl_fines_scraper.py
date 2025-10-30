# scraping monetary fines data from NFL.com

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import sys
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from io import StringIO

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from ingestion.storage import GCSWriter 
load_dotenv()

url = 'https://operations.nfl.com/inside-football-ops/rules-enforcement/gameday-accountability/'
data = requests.get(url).text
soup = BeautifulSoup(data, 'html.parser')

# All tables partitioned by week
weekResults = {} 
for div in soup.find_all('div', class_='select-table__table'):
    week = div.get('data-week') # this is the week number 

    weekSummary = {} 
    for p in div.find_all('p'):
        text = p.get_text()
        if ':' in text:
            key, val = text.split(':', 1)
            weekSummary[key.strip()] = val.strip()
    
    playerDetails = div.find('table')
    rows = []
    if playerDetails:
        for tr in playerDetails.find_all('tr'):
            row = [td.get_text(strip=True) for td in tr.find_all(['th', 'td'])]
            if row: 
                rows.append(row)
    
    weekResults[week] = {'weekSummary': weekSummary, 'playerDetails': rows}

# convert to ndJSON for BQ
finesRecords = []

for week, content in weekResults.items():
    playerDetails = content.get("playerDetails", [])
    if not playerDetails:
        continue

    headers = playerDetails[0]
    for row in playerDetails[1:]:
        if len(row) == len(headers):
            record = dict(zip(headers, row))
            record.update({
                'week': week,
                'weekSummary': content['weekSummary'],
                'scraped_at': datetime.now(timezone.utc).isoformat(),
                'source_url': url
            })
            finesRecords.append(record)

ndjson_data = "\n".join(json.dumps(r) for r in finesRecords)

# GCS

from ingestion.config import get_gcs_config
config = get_gcs_config()
path = config.get_raw_path("nfl", "fines")

gcs_writer = GCSWriter(bucket_name=config.bucket_name, project_id=config.project_id)
gcs_uri = gcs_writer.write_raw_data(
    data=ndjson_data,
    path=path,
    filename='nfl_fines.ndjson',
    include_timestamp=True
)
print(ndjson_data)





