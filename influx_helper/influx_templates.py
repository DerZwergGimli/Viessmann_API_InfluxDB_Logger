def json_influx_template(measurement, time, device_id,  value=0.0, unit='empty', status='empty'):
    json_database_body = [
        {
            "measurement": measurement,
            "tags": {
                "deviceId": device_id,
                "region": "us-west"
            },
            "time": time,
            "fields": {
                "value": value,
                "unit": unit,
                "status": status
            }
        }
    ]
    print(json_database_body)
    return json_database_body
