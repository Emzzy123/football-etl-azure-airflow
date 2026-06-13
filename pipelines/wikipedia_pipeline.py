import json
import os
import time

import pandas as pd
from geopy import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text

    except requests.RequestException as e:
        raise RuntimeError(f"Failed to get Wikipedia page: {e}")


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    if not html:
        raise ValueError("No HTML content received from Wikipedia")

    soup = BeautifulSoup(html, "html.parser")

    table = soup.select_one("table.wikitable.sortable")

    if table is None:
        raise ValueError("No table found with class 'wikitable sortable'")

    table_rows = table.find_all("tr")

    return table_rows


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


# pipelines/wikipedia_pipeline.py

def load_geocode_cache():
    cache_path = "/opt/airflow/data/geocode_cache.json"

    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as file:
            return json.load(file)

    return {}


def save_geocode_cache(cache):
    cache_path = "/opt/airflow/data/geocode_cache.json"

    with open(cache_path, "w", encoding="utf-8") as file:
        json.dump(cache, file, indent=2)


def get_lat_long(country, city, cache):
    location_key = f"{city}, {country}".lower().strip()

    if location_key in cache:
        cached_value = cache[location_key]

        if cached_value.get("latitude") is not None and cached_value.get("longitude") is not None:
            return cached_value["latitude"], cached_value["longitude"]

    geolocator = Nominatim(
        user_agent="football-data-engineering-project",
        timeout=20
    )

    try:
        location = geolocator.geocode(f"{city}, {country}")

        time.sleep(1)

        if location:
            latitude = location.latitude
            longitude = location.longitude

            cache[location_key] = {
                "latitude": latitude,
                "longitude": longitude
            }

            save_geocode_cache(cache)

            return latitude, longitude

        print(f"No geocode result found for {city}, {country}")
        return None, None

    except Exception as error:
        print(f"Geocoding failed for {city}, {country}: {error}")
        return None, None


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)

    stadiums_df['images'] = stadiums_df['images'].apply(
        lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE
    )

    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    cache = load_geocode_cache()

    stadiums_df[['latitude', 'longitude']] = stadiums_df.apply(
        lambda x: pd.Series(get_lat_long(x['country'], x['city'], cache)),
        axis=1
    )

    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')
    
    os.makedirs('/opt/airflow/data', exist_ok=True)

    data.to_csv('/opt/airflow/data/' + file_name, index=False)

    data.to_csv('abfs://footballdataeng@footballdataengsa.dfs.core.windows.net/data/' + file_name,
                storage_options={
                    'account_key': os.getenv("AZURE_STORAGE_KEY")
                }, index=False)

    return "OK"
