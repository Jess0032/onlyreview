import os

from aiohttp import ClientSession

from src.GeoIP_Updater.configuration import headers, url_files_to_update, url_latest_date

import logging

from src.GeoIP_Updater.download_files import download_files, get_last_update, unzip_files
from src.GeoIP_Updater.elasticsearch.elasticsearch import bulk_data

logging.basicConfig(level=logging.WARNING)


async def periodic_check(dirs):
    try:
        last_update_date_file = "last_update_date"

        async with ClientSession(headers=headers) as session:
            last_update = await get_last_update(url_latest_date, session)
            if last_update == get_local_update_date(last_update_date_file):
                return
            logging.info("New update available, downloading...")

            zip_path = "geoip.zip"
            await download_files(url=url_files_to_update, zipfile=zip_path)
            set_local_update_date(last_update_date_file, last_update)
            logging.info("Download complete.")

            unzipped_folder = "unzipped_folder"
            unzip_files(zip_path, unzipped_folder)
            os.remove(zip_path)
            logging.info("Unzip complete.")

            bulk_data(unzipped_folder)

    except Exception as e:
        logging.exception(e)


def get_local_update_date(file):
    with open(file, 'r') as f:
        return f.read().strip()


def set_local_update_date(file, date):
    with open(file, 'w') as f:
        f.write(date)
