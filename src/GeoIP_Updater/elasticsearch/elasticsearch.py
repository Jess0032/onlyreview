import logging
import os
import ipaddress
import pandas as pd
import numpy as np
from opensearchpy import helpers

from src.GeoIP_Updater.configuration import index_name
from src.GeoIP_Updater.elasticsearch import es


def create_actions(client, df_locations_info, chunk):
    actions = []
    for row in chunk.itertuples():

        doc = create_document(df_locations_info, row)

        action = {
            '_index': index_name,
            '_source': {"doc": doc}}

        if doc_id := check_if_document_exists(client, doc):
            action['_id'] = doc_id
            action['_op_type'] = 'update'
            print("update")

        else:
            action['_op_type'] = 'index'
            print("index")

        actions.append(action)
    return actions


def create_document(df_locations_info, row):
    geoname_id = row.geoname_id

    doc = {
        "network": str(row.network),
        "latitude": str(row.latitude),
        "longitude": str(row.longitude),
        "cityName": None,
        "continentCode": None,
        "continentName": None,
        "countryIsoCode": None,
        "countryName": None,
        "localeCode": None,
        "metroCode": None,
        "subdivision1IsoCode": None,
        "subdivision1IsoName": None,
        "subdivision2IsoCode": None,
        "subdivision2IsoName": None,
        "timeZone": None,
    }
    if geoname_id in df_locations_info.index:
        doc["cityName"] = str(df_locations_info.loc[geoname_id].city_name)
        doc["continentCode"] = str(df_locations_info.loc[geoname_id].continent_code)
        doc["continentName"] = str(df_locations_info.loc[geoname_id].continent_name)
        doc["countryIsoCode"] = str(df_locations_info.loc[geoname_id].country_iso_code)
        doc["countryName"] = str(df_locations_info.loc[geoname_id].country_name)
        doc["localeCode"] = str(df_locations_info.loc[geoname_id].locale_code)
        doc["metroCode"] = str(df_locations_info.loc[geoname_id].metro_code)
        doc["subdivision1IsoCode"] = str(df_locations_info.loc[geoname_id].subdivision_1_iso_code)
        doc["subdivision1IsoName"] = str(df_locations_info.loc[geoname_id].subdivision_1_name)
        doc["subdivision2IsoCode"] = str(df_locations_info.loc[geoname_id].subdivision_2_iso_code)
        doc["subdivision2IsoName"] = str(df_locations_info.loc[geoname_id].subdivision_2_name)
        doc["timeZone"] = str(df_locations_info.loc[geoname_id].time_zone)

    return doc


def check_if_document_exists(client, document):
    try:
        ip_network = ipaddress.ip_network(document["network"])
        search_query = {
            "query": {
                "bool": {
                    "must": [
                        {"query_string": {"query": {'network': str(document["network"])}}},
                        {"match": {'latitude': document["latitude"]}},
                        {"match": {'longitude': document["longitude"]}}
                    ]
                }
            }
        }

        search_response = client.search(index=index_name, body=search_query)
        print(search_response)
        if search_response["hits"]["total"]["value"] > 0:
            return search_response["hits"]["hits"][0]["_id"]

        else:
            return False

    except Exception as e:
        print(e)


def bulk_data(geoip_folder):
    documents_block_list = ["blocks-v5.csv"]
    locations_info = "locations-en.csv"

    try:
        df_locations_info = pd.read_csv(os.path.join(geoip_folder, locations_info), index_col='geoname_id',
                                        dtype={"city_name": str, "continent_code": str, "continent_name": str,
                                               "country_iso_code": str, "country_name": str, "locale_code": str,
                                               "metro_code": str, "subdivision_1_iso_code": str,
                                               "subdivision_1_name": str,
                                               "subdivision_2_iso_code": str, "subdivision_2_name": str,
                                               "time_zone": str})

        df_locations_info.replace({np.nan: None}, inplace=True)

        for document in documents_block_list:
            for chunk in pd.read_csv(os.path.join(geoip_folder, document), chunksize=7000):
                response = helpers.bulk(client=es, actions=create_actions(es, df_locations_info, chunk),
                                        stats_only=False)
                logging.info(f"Successfully inserted {response[0]} documents")
                if response[1]:
                    errors = '\n'.join(response[1])
                    logging.error(f"Failed to insert {len(response[1])} documents, list of errors: {errors}")

    except Exception as e:
        logging.error(e)


bulk_data("unzipped_folder")
