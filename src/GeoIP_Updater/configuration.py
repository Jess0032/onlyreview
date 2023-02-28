import os

host = os.getenv('HOST')
url_latest_date = os.getenv('URL_LATEST_DATE')
url_files_to_update = os.getenv('URL_FILES_TO_UPDATE')
minutes = os.getenv('MINUTES', 1)
headers = os.getenv('HEADERS', None)
index_name = '.utm-geoip'
base_dir = './'
