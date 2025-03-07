import os
import tempfile
import requests
import tarfile
from typing import List,Dict,Any
import shutil
import gzip
from fetch.config import config


def download_and_extract_gz(
    url,
    output_folder = config["data_output"],
    ):

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:

        # Get filename from URL
        filename = os.path.basename(url)
        file_path = os.path.join(temp_dir, filename)

        #gz_file_path = os.path.join(temp_dir, "downloaded_file.gz")

        # Download the file
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
        else:
            raise Exception(f"Failed to download file: {response.status_code}")

        #Create output folder in repo
        os.makedirs(output_folder, exist_ok=True)

        #extract files
        is_tar = tarfile.is_tarfile(file_path)

        if is_tar:
            with tarfile.open(file_path, "r:gz") as tar:
                    tar.extractall(path=output_folder)
            print(f"Extracted tar file {url} to: {output_folder}")

        elif filename.endswith(".gz"):
            extracted_file_path = os.path.join(output_folder, os.path.splitext(filename)[0])  # Remove .gz extension
            with gzip.open(file_path, "rb") as gz_file:
                with open(extracted_file_path, "wb") as extracted_file:
                    shutil.copyfileobj(gz_file, extracted_file)
            print(f"Extracted gz file {url} to: {output_folder}")

        else:
            raise Exception("Unsupported file format")


def get_source_files(urls:List = config["fetch_urls"]):
    
    #check if any URLs provuded
    if not urls:
        print("No URLs provided")

    #Loop extract files
    try:
        for url in urls:
            download_and_extract_gz(url)

    except Exception as e:
        print("An error occurred:", e)
    


