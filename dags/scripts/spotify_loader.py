import psycopg2
import pandas as pd
import datetime
import os

# fetch credentials from environment variables 
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

def load_data_to_postgres(csv_file, table_name):
    """
    Load csv data into PostgreSQL table
    """
    # connect to database
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connected to the database.")
        cur = conn.cursor()

        cur.execute("SET search_path TO public;") # explicitly set the PostgreSQL schema search path to public

        # read transformed csv file
        df = pd.read_csv(csv_file)
        print(f"Data read successfully. len(df) = {len(df)}")

        # Insert data row by row
        for _, row in df.iterrows():
            cur.execute(
                f"""
                INSERT INTO public.{table_name} (
                    album_type, 
                    artists, 
                    available_markets, 
                    external_urls, 
                    href, 
                    id, 
                    images, 
                    name, 
                    release_date, 
                    release_date_precision, 
                    total_tracks, 
                    type, 
                    uri, 
                    spotify_available_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row['album_type'],
                    row['artists'],
                    row['available_markets'],
                    row['external_urls'],
                    row['href'],
                    row['id'],
                    row['images'],
                    row['name'],
                    row['release_date'],
                    row['release_date_precision'],
                    row['total_tracks'],
                    row['type'],
                    row['uri'],
                    row['spotify_available_date']
                )
            )

        # Commit changes and close the connection
        conn.commit()
        cur.close()
        print("Data loaded successfully.")

    except Exception as e:
        print(f"Error: {e}")

        if conn:
            # NOTE: if INSERT statement fails for some reason, subsequent queries will not be possible unless we 'rollback' the failed sql option.
            # Error: "psycopg2.errors.InFailedSqlTransaction: current transaction is aborted, commands ignored until end of transaction block"
            # Queries in 'finally' section will not run without rollback.
            conn.rollback()

    finally:
        if conn:
            ########### Some queries to check on status of DB
            # check distinct dates in db
            query = "SELECT DISTINCT spotify_available_date FROM new_releases"
            cur.execute(query)
            distinct_dates = cur.fetchall()
            print("Distinct dates in the database:", distinct_dates)

            # check number of rows in db
            query = "SELECT count(*) FROM new_releases"
            cur.execute(query)
            num_rows = cur.fetchall()
            print("Number of rows in the database:", num_rows)

            # sample 5 rows from db
            query = "SELECT * FROM new_releases LIMIT 5"
            cur.execute(query)
            limit_5 = cur.fetchall()
            print("Limit 5:", limit_5)

            # Example of analytical query: count number of new releases each day (will be limited to 50 as we capped the number of extractions from api to be 50)
            query = """
                select spotify_available_date, count(distinct id) as num_new_releases
                from new_releases
                group by spotify_available_date;
            """
            cur.execute(query)
            num_new_releases = cur.fetchall()
            print("num_new_releases:", num_new_releases)
        
        # close the connection
            conn.close()


def load_data():
    """
    This script will load the transformed csv data in the processed directory to the PostgreSQL database.
    """
    # create current date and path to processed csv file
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    processed_file = f"/opt/airflow/data/processed/transformed_data_{today_date}.csv"

    # pass date and file path into function
    load_data_to_postgres(processed_file, "new_releases")
