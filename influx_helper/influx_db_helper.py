import socket

import influxdb.exceptions
import requests.exceptions
import urllib3.exceptions

from file_helper import file_helper
from influxdb import InfluxDBClient
from influx_helper import influx_templates
from loguru import logger


def write_viessmann_data_to_influx_db(inlfux_db_file_path: str, json_viessmann_data):
    json_influx = file_helper.read_file_to_json(inlfux_db_file_path)
    b_write_to_db = False
    try:
        client = InfluxDBClient(json_influx["credentials"]["address"],
                                json_influx["credentials"]["port"],
                                json_influx["credentials"]["user"],
                                json_influx["credentials"]["password"],
                                json_influx["credentials"]["database_name"],
                                timeout=1)

        client.create_database(json_influx["credentials"]["database_name"])
        b_write_to_db = True
    except (socket.timeout, urllib3.exceptions.ConnectTimeoutError, requests.exceptions.ConnectTimeout):
        logger.error("Timeout when trying to connect to InfluxDB")
    except requests.exceptions.ConnectionError:
        logger.error("ConnectionError when trying to connect to InfluxDB")

    if b_write_to_db:
        try:
            for data_point in json_viessmann_data['data']:
                if len(data_point.get('properties')) != 0:
                    fields = {}
                    for property in data_point.get("properties"):
                        fields[str(property)] = data_point.get("properties").get(str(property)).get("value")
                    tags = {"isEnabled": data_point.get("isEnabled"),
                            "isReady": data_point.get("isReady"),
                            "gatewayId": data_point.get("gatewayId"),
                            "apiVersion": data_point.get("apiVersion")}

                    json_database_body = influx_templates.json_influx_template_modular(
                        measurement=data_point.get("feature"),
                        time=data_point.get("timestamp"),
                        tags=tags,
                        fields=fields
                    )
                   # value = 0.0
                   # unit = "empty"
                   # status = "none"
                   # if data_point.get('properties').get('value'):
                   #     value = data_point.get('properties').get('value').get('value')
                   # if data_point.get('properties').get('unit'):
                   #     unit = data_point.get('properties').get('unit').get('value')
                   # if data_point.get('properties').get('status'):
                   #     status = data_point.get('properties').get('status').get('value')
                   # json_database_body = influx_templates.json_influx_template(data_point.get('feature'), data_point.get('timestamp'), data_point.get('deviceId'), value)
                    try:
                        client.write_points(json_database_body)
                    except influxdb.exceptions.InfluxDBClientError:
                        logger.warning("Data was dropped - already written?")
                    print(json_database_body)
        except TypeError:
            logger.warning("Error fetching data - fetched datapoint may be empty")

