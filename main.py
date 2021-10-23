from viessmann_helper import viessmann_helper
from influx_helper import influx_db_helper
from loguru import logger

#Globals
viessmann_api_file_path = "configs/viessmann_api.json"
inlfux_db_file_path = "configs/influx_db.json"


def menu():
    print("============================================")
    print("Welcome to the Vissman API extractor")
    print("1 = CreateToken")
    print("2 = GetToken")
    print("3 = UpdateToken")
    print("4 = installation_id")
    print("5 = get_gateway_serial")
    print("6 = get_device_id")
    print("7 = get_features_list_all")
    print("8 = get_features_form_list_by_device")
    print("9 = write_viessmann_data_to_influx_db")
    print("0 = exit")
    print("============================================")
    case = input("Enter NUMBER:")

    if case == "1":
        viessmann_helper.token_authorize(viessmann_api_file_path)
    elif case == "2":
        viessmann_helper.get_token(viessmann_api_file_path)
    elif case == "3":
        viessmann_helper.get_update_token(viessmann_api_file_path)
    elif case == "4":
        viessmann_helper.installation_id(viessmann_api_file_path)
    elif case == "5":
        viessmann_helper.get_gateway_serial(viessmann_api_file_path)
    elif case == "6":
        viessmann_helper.get_device_id(viessmann_api_file_path)
    elif case == "7":
        viessmann_helper.get_features_list_all(viessmann_api_file_path)
    elif case == "8":
        viessmann_helper.get_features_form_list_by_device(viessmann_api_file_path, "0")
    elif case == "9":
        influx_db_helper.write_viessmann_data_to_influx_db(inlfux_db_file_path, viessmann_helper.get_features_form_list_by_device(viessmann_api_file_path, "0"))
    elif case == "0":
        quit()
    menu()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logger.add("log_file.log", rotation="10 MB", colorize=True, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")
    logger.info("TESTLog entry")
    menu()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
