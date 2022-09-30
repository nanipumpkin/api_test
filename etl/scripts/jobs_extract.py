import os
import csv
import tempfile
from zipfile import ZipFile

import requests

# Settings
base_path = os.path.abspath(__file__ + "/../../")

# START - Paths for new February 2021 data available

# External website file url
source_url = "https://www.linkedin.com/jobs/search?keywords=20data%20analyst&location=Florianopolis&refresh=true"

# Source path where we want to save the .zip file downloaded from the website
source_path = f"data/source/downloaded_at=2022-09-01/job-list.zip"

# Raw path where we want to extract the new .csv data
raw_path = f"data/source/downloaded_at=2022-09-01/job-list.csv"

# END - Paths for new February 2021 data available


def create_folder_if_not_exists(path):
    """
    Create a new folder if it doesn't exists
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)


def download_snapshot():
    """
    Download the new dataset from the source
    """
    create_folder_if_not_exists(source_path)
    with open(source_path, "wb") as source_joblist:
        response = requests.get(source_url, verify=False)
        source_joblist.write(response.content)


def save_new_raw_data():
    """
    Save new raw data from the source
    """

    create_folder_if_not_exists(raw_path)
    with tempfile.TemporaryDirectory() as dirpath:
        with ZipFile(
            source_path,
            "r",
        ) as zipfile:
            names_list = zipfile.namelist()
            csv_file_path = zipfile.extract(names_list[0], path=dirpath)
            # Open the CSV file in read mode
            with open(csv_file_path, mode="r", encoding="windows-1252") as csv_file:
                reader = csv.DictReader(csv_file)

                row = next(reader)  # Get first row from reader
                print("[Extract] First row example:", row)

                # Open the CSV file in write mode
                with open(
                    raw_path,
                    mode="w",
                    encoding="windows-1252"
                ) as csv_file:
                    # Rename field names so they're ready for the next step
                    fieldnames = {
                        "Date of Sale (dd/mm/yyyy)": "date_of_sale",
                        "Address": "address",
                        "Postal Code": "postal_code",
                        "County": "county",
                        "Price (€)": "price",
                        "Description of Property": "description",
                    }
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    # Write headers as first line
                    writer.writerow(fieldnames)
                    for row in reader:
                        # Write all rows in file
                        writer.writerow(row)

# Main function called inside the execute.py script
def main():
    if __name__ == "main":
        print("[Extract] Start")
        create_folder_if_not_exists()
        print("[Extract] Downloading snapshot")
        download_snapshot()
        print(f"[Extract] Saving data from '{source_path}' to '{raw_path}'")
        save_new_raw_data()
        print(f"[Extract] End")