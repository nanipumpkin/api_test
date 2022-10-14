import os
import csv
from datetime import datetime

from script.common.tables import JobRawAll
from script.common.base import session
from sqlalchemy import text

# Settings
BASE_PATH = os.path.abspath(__file__ + '/../')
DATA_PATH = f'{BASE_PATH}/data/'
RAW_PATH = DATA_PATH + 'raw/'


def conversor_a():
    """
    Converts html data into csv using beautifulsoup
    """
    return input_string.capitalize()


def conversor_b():
    """
    Converts html data into csv using selenium
    """
    current_format = datetime.strptime(date_input, "%d/%m/%Y")
    new_format = current_format.strftime("%Y-%m-%d")
    return new_format


def truncate_table():
    """
    Ensure that "job_raw_all" table is always in empty state before running any transformations.
    And primary key (id) restarts from 1.
    """
    session.execute(
        text("TRUNCATE TABLE job_raw_all;ALTER SEQUENCE job_raw_all_id_seq RESTART;")
    )
    session.commit()


def transform_and_save_new_data():
    """
    Apply all transformations for each row in the .html file before saving it into database
    """
    with open(RAW_PATH, mode="r", encoding="windows-1252") as csv_file:
        # Read the new .csv snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our PprRawAll objects
        job_raw_objects = []
        for row in reader:
            # Apply transformations and save as PprRawAll object
            job_raw_objects.append(
                JobRawAll(
                    title = update_title(row['title'])
                    date_posted = update_date_posted(row['date_posted'])
                )
            )
        # Bulk save all new processed objects and commit
        session.bulk_save_objects(job_raw_objects)
        session.commit()

# Main function called inside the execute.py script
def main():
    print("[Transform] Start")
    print("[Transform] Remove any old data from job_raw_all table")
    truncate_table()
    print("[Transform] Transform new data available in job_raw_all table")
    transform_and_save_new_data()
    print("[Transform] End")