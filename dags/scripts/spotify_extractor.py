import requests
import json
import os
import datetime
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()

# api credentials
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# Function to authenticate and get access token
def get_access_token():
    """
    Obtain access token to interact with API
    """
    url = "https://accounts.spotify.com/api/token"

    # NOTE: https://stackoverflow.com/questions/4007969/application-x-www-form-urlencoded-or-multipart-form-data
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    
    # obtain access token
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response
    else:
        raise Exception(f"Failed to get access token: {response.text}")

def fetch_new_releases_paged(token=None, max_pages=5, limit=10, offset=0):
    """
    Uses the access token to fetch new releases from Spotify API. Due to storage constraints, we limit it to 50 items (5 pages * 10 items) at most.
    """
    headers = {"Authorization": f"Bearer {token}"}
    all_data = []

    for i in range(max_pages):
        """ NOTE
        offset: The index of the first item to return. Default: 0 (the first item). Use with limit to get the next set of items.
        spotify api doc: https://developer.spotify.com/documentation/web-api/reference/get-new-releases

        we run a loop 5 times (due to storage constraints). each iteration extracts 10 items.
        """
        offset = i * limit
        url = f"https://api.spotify.com/v1/browse/new-releases?limit={limit}&offset={offset}"

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # return response.json()
            data = response.json()
            all_data.extend(data["albums"]["items"]) 
            # NOTE: data['albums']['items'] is a list of size 10 since limit = 10.
            # each item in the list is a dictionary.
        else:
            raise Exception(f"Error on page {i}, failed to fetch new releases: {response.text}")
    return all_data

def save_to_staging(data):
    """
    Saves the fetched data into .json files in local file storage
    """
    # create timestamp
    extraction_date = datetime.datetime.now().strftime("%Y-%m-%d")
    final_data = {
        "extraction_date": extraction_date,
        "raw_data": data
    }
    
    # save to staging area
    output_dir = "/opt/airflow/data/staging" # NOTE: need to include /opt/airflow/ in directory path because os.getcwd() is /opt/airflow
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/new_releases_{extraction_date}.json" # NOTE: we name the raw data file as new_releases_extraction_date.json
    with open(output_file, "w") as f:
        json.dump(final_data, f, indent=4)
    print(f"Raw data saved to {output_file}")

# main function for dag
def extract_data():
    """
    Extracts raw data from API on a daily level and saves into staging area as json format.
    """
    try:
        response = get_access_token()
        token = response.json()["access_token"]

        all_data = fetch_new_releases_paged(token=token)
        print(f"len(all_data) = {len(all_data)}")

        save_to_staging(all_data)
        
    except Exception as e:
        print(e)