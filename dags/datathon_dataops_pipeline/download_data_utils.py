import requests
import logging
import gzip
import shutil
import os

# Define logger
logger = logging.getLogger(__name__)


def download_url(url: str, save_path: str, chunk_size: int = 128) -> None:
    logger.info(f"Downloading data from {url}...")
    try:
        r = requests.get(url, stream=True)
        with open(save_path, "wb") as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)

        logger.info(f"Data from {url} downloaded OK")
    except Exception as e:
        logger.error(f"Fail downloading data from {url}")
        logger.error(e)


def extract_gz(extract_path: str, file_path: str) -> None:
    logger.info(f"Extracting {extract_path}...")
    try:
        with gzip.open(extract_path, "rb") as f_in:
            with open(file_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        logger.info(f"{extract_path} extracted OK and saved on {file_path}")
    except Exception as e:
        logger.error(f"Fail extracting data from {extract_path}")
        logger.error(e)


def download_data() -> None:

    # Create necessary directories
    if not os.path.isdir("gz_data"):
        os.mkdir("gz_data")

    if not os.path.isdir("extracted_data"):
        os.mkdir("extracted_data")

    # Define necessary URLs
    urls = [
        "https://datasets.imdbws.com/name.basics.tsv.gz",
        "https://datasets.imdbws.com/title.basics.tsv.gz",
        "https://datasets.imdbws.com/title.crew.tsv.gz",
        "https://datasets.imdbws.com/title.ratings.tsv.gz",
    ]

    for url in urls:
        # Define file name from URL
        gz_file_name = url.replace("https://datasets.imdbws.com/", "")
        output_file_name = gz_file_name.replace(".gz", "")

        # Download and extract gz file
        download_url(url, f"gz_data/{gz_file_name}")
        extract_gz(f"gz_data/{gz_file_name}", f"extracted_data/{output_file_name}")

    # Clean gz directory
    shutil.rmtree("gz_data")
    logger.info("Directory cleaned up")

    logger.info("Download data task OK")
