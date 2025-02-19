import json

import pandas as pd
from geopy import Nominatim
import geocoder

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    import requests

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    print("Getting Wikipedia page...", url)

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Check for HTTP errors

        # Print the first 500 characters of the response
        print("HTML Response Snippet:\n", response.text[:500])  

        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None



def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')

    # Find the first table that contains 'wikitable'
    table = soup.find_all("table", {"class": "wikitable"})[1]


    if not table:
        raise ValueError("No 'wikitable' table found on the Wikipedia page!")

    return table.find_all('tr')





def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    print(rows)
    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "OK"


   

def get_lat_long(country, city):
    """Fetch latitude and longitude using ArcGIS geocoding service."""
    try:
         # Add delay to avoid hitting rate limits
        location = geocoder.arcgis(f'{city}, {country}')
        
        if location and location.ok:
            return location.latlng[0], location.latlng[1]  # Return latitude & longitude

        return None  # Return None if location not found
    except Exception as e:
        print(f"Error fetching coordinates for {city}, {country}: {e}")
        return None  # Return None if any error occurs



def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    data.to_csv('data/' + file_name, index=False)
    # data.to_csv('abfs://footballdataeng@footballdataeng.dfs.core.windows.net/data/' + file_name,
    #             storage_options={
    #                 'account_key': 'pcrbWAsuPmzOH43lu1xang05pIs+g1Lys/bor0z59O38sVyWQNQ64AtEveMobZ2pIwCjqximReKY+ASt9dP/+A=='
    #             }, index=False)