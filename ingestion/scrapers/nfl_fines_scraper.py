# scraping monetary fines data from NFL.com

import requests
from bs4 import BeautifulSoup
import json
import sys
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from ingestion.storage import GCSWriter
from ingestion.config import get_gcs_config

load_dotenv()


class NFLFinesScraper:
    """
    Scrape NFL fines data from NFL Gameday Accountability page
    """
    URL = 'https://operations.nfl.com/inside-football-ops/rules-enforcement/gameday-accountability/'

    def scrape(self):
        
        data = requests.get(self.URL).text
        soup = BeautifulSoup(data, 'html.parser')
        
        weekResults = {} 
        for div in soup.find_all('div', class_='select-table__table'):
            week = div.get('data-week') # this is the week number for partitioning

            weekSummary = {} 
            for p in div.find_all('p'): # the summary info is enclosed in <p> tags
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
            playerDetails = content.get("playerDetails", []) # get the player details for the week
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
                        'source_url': self.URL
                    })
                    finesRecords.append(record)

        return finesRecords

    def scrape_and_write(self, gcs_writer=None):
        finesRecords = self.scrape()
        ndjson_data = "\n".join(json.dumps(r) for r in finesRecords)

        if gcs_writer is None:
            config = get_gcs_config()
            gcs_writer = GCSWriter(bucket_name=config.bucket_name, project_id=config.project_id)

        config = get_gcs_config()
        path = config.get_raw_path("nfl", "fines")

        gcs_uri = gcs_writer.write_raw_data(
            data=ndjson_data,
            path=path,
            filename='nfl_fines.ndjson',
            include_timestamp=True
        )

        return gcs_uri

if __name__ == "__main__":
    scraper = NFLFinesScraper()
    gcs_uri = scraper.scrape_and_write()
    print(f"Scraped and wrote fines data to: {gcs_uri}") 





