import logging

import opensearchpy

from src.GeoIP_Updater.configuration import host

try:
    es = opensearchpy.OpenSearch(hosts=[host])
    logging.info("Connected to OpenSearch")
except Exception as ex:
    logging.error("Error:", ex)
