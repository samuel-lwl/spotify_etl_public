import os
import json
import pandas as pd
import datetime

def load_raw_data_by_date(staging_dir, target_date):
    """
    Load raw json data from the staging area based on date.
    """
    # NOTE: reminder: file format is "new_releases_YYYY-MM-DD.json"

    # identifies all .json files in the staging directory with the format "new_release_target_date"
    target_files = [
        os.path.join(staging_dir, f)
        for f in os.listdir(staging_dir)
        if f.endswith(".json") and f"new_releases_{target_date}" in f
    ]
    
    all_data = []
    for file in target_files:
        # loop over each file, do json.load() and add to all_data list.
        print(f"file = {file}")

        with open(file, "r") as f:
            raw_data = json.load(f)
            all_data.extend(raw_data['raw_data']) 
            # NOTE: need to index 'raw_data' keyword in dictionary, otherwise it will only load the keys 'extraction_date' and 'albums'
    print(f"Loaded {len(all_data)} records from {len(target_files)} file(s).")
    # NOTE: each element in all_data is a record.
    return all_data

def validate_data(data):
    """
    Perform basic data validation: check that each record has name, release_date, and artists.
    """
    valid_data = []
    invalid_data = []

    for record in data:
        # data is a list of records and we loop over it
        if "name" in record and "release_date" in record and "artists" in record:
            valid_data.append(record)
        else:
            invalid_data.append(record)
    
    print(f"Valid records: {len(valid_data)}, Invalid records: {len(invalid_data)}")
    return valid_data

def transform_data_dynamic(data, target_date):
    """
    Transform raw data into pandas dataframe format by converting all first-level keys into columns.
    The input data is a list of records.
    The output data is a pandas dataframe.
    """
    # create a list to store all transformed data (as dictionaries), each element represents 1 record
    transformed_data = []
    for record in data:
        # NOTE: for each record, we transform it and store it as a dictionary. then, append the dictionary to the list.
        transformed_record = {}
        for key, value in record.items():
            # NOTE: for each key, if its value is another dict/json/list (i.e nested information), we keep it as it is.
            if isinstance(value, (dict, list)):
                transformed_record[key] = json.dumps(value)  # NOTE: convert to string for easy storage via json.dumps()
            else:
                transformed_record[key] = value 
        transformed_data.append(transformed_record)
    
    # we can easily convert the list of dictionaries into a pandas dataframe
    transformed_data = pd.DataFrame(transformed_data)

    # NOTE: add the date we pulled the data from the API, this represents the date the records were made available on spotify.
    transformed_data['spotify_available_date'] = target_date
    return transformed_data

def save_transformed_data(df, processed_dir, target_date):
    """
    Save transformed data in pandas.Dataframe format to the processed directory in csv format.
    """
    os.makedirs(processed_dir, exist_ok=True)
    output_file = os.path.join(processed_dir, f"transformed_data_{target_date}.csv")
    df.to_csv(output_file, index=False)
    print(f"Transformed data saved to {output_file}")

# main function for dag
def transform_data():
    """
    Loads data from staging area in json format on a daily format, remove invalid data, transform into pandas.DataFrame, and save into processed directory in csv format.
    """
    staging_dir = "/opt/airflow/data/staging" # NOTE: remember to include /opt/airflow to path
    processed_dir = "/opt/airflow/data/processed"

    # get today's date
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")

    # load raw data json files
    raw_data = load_raw_data_by_date(staging_dir, today_date)

    # validate, transform, and save
    if raw_data:  # Proceed only if data is found
        validated_data = validate_data(raw_data)
        transformed_df = transform_data_dynamic(validated_data, today_date)
        save_transformed_data(transformed_df, processed_dir, today_date)
    else:
        print(f"No files found for {today_date}. Transformation skipped.")
