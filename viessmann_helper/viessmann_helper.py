from file_helper import file_helper
import json
import requests
from requests.structures import CaseInsensitiveDict
from loguru import logger


@logger.catch
def authorize_token(viessmann_api_file_path: str):
    logger.info("Running ViessmannAPI token authorize...")
    try:
        j_credentials = file_helper.read_file_to_json(viessmann_api_file_path)
        client_id = j_credentials['credentials']['client_id']
        redirect_uri = j_credentials['credentials']['redirect_uri']
        code_challenge = j_credentials['credentials']['code_challenge']
        scope = j_credentials['credentials']['scope']

        print("Please open the following link and paste the CODE in the terminal window!")
        req_url = "https://iam.viessmann.com/idp/v2/authorize?client_id=" + client_id + "&redirect_uri=" + redirect_uri + "&response_type=code&code_challenge=" + code_challenge + "&scope=" + scope
        print(req_url)

        code = input("Enter CODE:")
        j_credentials['credentials']['code'] = code
        file_helper.write_json_to_file(viessmann_api_file_path, j_credentials)
        return 0
    except:
        logger.error("Unable to Authorize ViessmannAPI Token")
        return 1


@logger.catch
def get_token(viessmann_api_file_path: str):
    logger.info("Getting ViessmannAPI TOKEN")
    j_credentials = file_helper.read_file_to_json(viessmann_api_file_path)
    client_id = j_credentials['credentials']['client_id']
    refresh_token = j_credentials['credentials']['refresh_token']
    redirect_uri = j_credentials['credentials']['redirect_uri']
    code_challenge = j_credentials['credentials']['code_challenge']
    code = j_credentials['credentials']['code']
    scope = j_credentials['credentials']['scope']

    req_url = "https://iam.viessmann.com/idp/v2/token"
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    req_data = "grant_type=authorization_code&client_id=" + client_id + "&redirect_uri=" + redirect_uri + "&code_verifier=" + code_challenge + "&code=" + code

    resp = requests.post(req_url, headers=headers, data=req_data)

    print(resp.status_code)
    print(resp.text)

    if resp.status_code == 200:
        resp_json = resp.json()
        print("AccessToken:  " + resp_json['access_token'])
        print("RefreshToken: " + resp_json['refresh_token'])
        j_credentials['credentials']['access_token'] = resp_json['access_token']
        j_credentials['credentials']['refresh_token'] = resp_json['refresh_token']
        file_helper.write_json_to_file(viessmann_api_file_path, j_credentials)
        return 0
    else:
        logger.error("Error when getting ViessmannAPI TOKEN")
        return 1


@logger.catch
def get_update_token(viessmann_api_file_path: str):
    logger.info("Updating ViessmannAPI TOKEN")
    j_credentials = file_helper.read_file_to_json(viessmann_api_file_path)
    client_id = j_credentials['credentials']['client_id']
    refresh_token = j_credentials['credentials']['refresh_token']
    redirect_uri = j_credentials['credentials']['redirect_uri']
    code_challenge = j_credentials['credentials']['code_challenge']
    code = j_credentials['credentials']['code']
    scope = j_credentials['credentials']['scope']
    refresh_token = j_credentials['credentials']['refresh_token']

    req_url = "https://iam.viessmann.com/idp/v2/token"
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    req_data = "grant_type=refresh_token&client_id=" + client_id + "&refresh_token=" + refresh_token
    print(req_data)

    resp = requests.post(req_url, headers=headers, data=req_data)

    print(resp.status_code)
    print(resp.text)

    if resp.status_code == 200:
        resp_json = resp.json()
        print("AccessToken:  " + resp_json['access_token'])
        print("RefreshToken: " + resp_json['refresh_token'])
        j_credentials['credentials']['access_token'] = resp_json['access_token']
        j_credentials['credentials']['refresh_token'] = resp_json['refresh_token']
        file_helper.write_json_to_file(viessmann_api_file_path, j_credentials)
        return 0
    else:
        print("Error occured!")
        return 1


@logger.catch
def get_installation_id(viessmann_api_file_path: str):
    j_data = file_helper.read_file_to_json(viessmann_api_file_path)
    url = j_data["setup"]["parent_url"] + "installations"
    print("Calling: " + url)
    response_json = make_request(viessmann_api_file_path, url)
    j_data["setup"]["installation_id"] = response_json["data"][0]["id"]
    file_helper.write_json_to_file(viessmann_api_file_path, j_data)


@logger.catch
def get_gateway_serial(viessmann_api_file_path: str):
    j_data = file_helper.read_file_to_json(viessmann_api_file_path)
    url = j_data["setup"]["parent_url"] + "gateways"
    print("Calling: " + url)
    response_json = make_request(viessmann_api_file_path, url)
    j_data["setup"]["gateway_serial_id"] = response_json["data"][0]["serial"]
    file_helper.write_json_to_file(viessmann_api_file_path, j_data)


@logger.catch
def get_device_id(viessmann_api_file_path: str):
    j_data = file_helper.read_file_to_json(viessmann_api_file_path)
    url = j_data["setup"]["parent_url"] + "installations/" + str(j_data["setup"]["installation_id"]) + "/gateways/" + str(j_data["setup"]["gateway_serial_id"]) + "/devices"
    print("Calling: " + url)
    response_json = make_request(viessmann_api_file_path, url)

    for idx, device in enumerate(response_json["data"]):
        j_device = {}
        try:
            if j_data["devices"][idx]["id"] != device["id"]:
                j_device["id"] = device["id"]
                j_device["features"] = []
                j_data["devices"].append(j_device)
        except IndexError:
            j_device["id"] = device["id"]
            j_device["features"] = []
            j_data["devices"].append(j_device)

    file_helper.write_json_to_file(viessmann_api_file_path, j_data)
    file_helper.print_json_pretty(j_data)


@logger.catch
def get_features_list_all(viessmann_api_file_path: str):
    j_data = file_helper.read_file_to_json(viessmann_api_file_path)
    for idx, device in enumerate(j_data["devices"]):
        print(device["id"])
        url = j_data["setup"]["parent_url"] + "installations/" + str(j_data["setup"]["installation_id"]) + "/gateways/" + str(j_data["setup"]["gateway_serial_id"]) + "/devices/" + str(device["id"]) + "/features"
        print("Calling: " + url)
        response_json = make_request(viessmann_api_file_path, url)
        features = []
        for feature in response_json["data"]:
            print(feature["feature"])
            features.append(feature["feature"])
        device["features"] = features

    file_helper.write_json_to_file(viessmann_api_file_path, j_data)
    file_helper.print_json_pretty(j_data)


@logger.catch
def get_features_form_list_by_device(viessmann_api_file_path: str, device_name: str):
    j_data = file_helper.read_file_to_json(viessmann_api_file_path)
    url = j_data["setup"]["parent_url"] + "installations/" + str(j_data["setup"]["installation_id"]) + "/gateways/" + str(j_data["setup"]["gateway_serial_id"]) + "/devices/" + str(device_name) + "/features"
    url += "?regex="
    for feature in j_data["devices"]:
        if feature["id"] == device_name:
            for idx, topic_string in enumerate(feature["features"]):
                if idx == 0:
                    url += str(topic_string)
                else:
                    url += "%7C" + str(topic_string)
                print(topic_string)
    print(url)
    return make_request(viessmann_api_file_path, url)


@logger.catch
def get_features_form_list_all(viessmann_api_file_path: str):
    j_data = file_helper.read_file_to_json(viessmann_api_file_path)
    for device in j_data["devices"]:
        print(device["id"])
        get_features_form_list_by_device(viessmann_api_file_path, device["id"])


@logger.catch
def make_request(viessmann_api_file_path: str, url: str):
    logger.info("Making request to Viessmann API")
    req_header = CaseInsensitiveDict()
    f_credentials = open(viessmann_api_file_path, "r")
    j_credentials = json.load(f_credentials)
    token = j_credentials['credentials']['access_token']
    req_header["Authorization"] = "Bearer " + str(token)
    f_credentials.close()

    response = requests.get(url, headers=req_header)
    logger.info("ViessmannAPI: Request Status-Code=" + str(response.status_code))
    if response.status_code == 200:
        response_json = response.json()
        print(json.dumps(response_json, indent=4, sort_keys=True))
        return response_json
    elif response.status_code == 401:
        logger.warning("Access token expired - creating a new one...")
        if (j_credentials["credentials"]["auto_refresh_token"]):
            get_update_token(viessmann_api_file_path)
            make_request(viessmann_api_file_path, url)
    else:
        logger.error("Unable to handle Viessmann API error")
        response_json = response.json()
        print(json.dumps(response_json, indent=4, sort_keys=True))
        return 1

