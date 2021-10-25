import influx_helper.influx_templates
from viessmann_helper import viessmann_helper
from influx_helper import influx_db_helper
from loguru import logger
import sys
import getopt
import time
from tqdm import tqdm

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
    print("10 = write_viessmann_data_to_influx_db [once and exit]")
    print("0 = exit")
    print("============================================")
    case = input("Enter NUMBER:")

    if case == "1":
        viessmann_helper.authorize_token(viessmann_api_file_path)
    elif case == "2":
        viessmann_helper.get_token(viessmann_api_file_path)
    elif case == "3":
        viessmann_helper.get_update_token(viessmann_api_file_path)
    elif case == "4":
        viessmann_helper.get_installation_id(viessmann_api_file_path)
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
    elif case == "10":
        influx_db_helper.write_viessmann_data_to_influx_db(inlfux_db_file_path, viessmann_helper.get_features_form_list_by_device(viessmann_api_file_path, "0"))
        quit()
    elif case == "0":
        quit()
    menu()


def arguments(argv):
    sleep_time = ''
    try:
        opts, args = getopt.getopt(argv, "hrs:", ["rmode=", "stime="])
    except getopt.GetoptError:
        print('python_viessmannapi.py -h')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('--> Help (python_viessmannapi.py):')
            print('python_viessmannapi.py -r auto \t[to run without user interaction]')
            print('python_viessmannapi.py -s 10 \t\t[to run without user interaction - time based (seconds)]')
            print('python_viessmannapi.py \t\t[to run with user interaction]')
            sys.exit()
        elif opt in ("-r", "--mode"):
            print("running in auto mode")
            influx_db_helper.write_viessmann_data_to_influx_db(inlfux_db_file_path, viessmann_helper.get_features_form_list_by_device(viessmann_api_file_path, "0"))
            quit()
        elif opt in ("-s", "--time"):
            print("Running in sleep mode: [auto]")
            sleep_time = arg
            print("sleep_time=" + str(sleep_time))

            while True:
                influx_db_helper.write_viessmann_data_to_influx_db(inlfux_db_file_path, viessmann_helper.get_features_form_list_by_device(viessmann_api_file_path, "0"))
                with tqdm(total=int(sleep_time)) as pbar:
                    for i in range(int(sleep_time)):
                        time.sleep(1)
                        pbar.update(1)
            quit()
    menu()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logger.add("log_file.log", rotation="10 MB", colorize=True, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}")
    logger.info("Started...")
    influx_helper.influx_templates.json_influx_template_modular("api.key.name", "1234566:00", {"tag1":"name", "tag2":"name"}, {"tag1":"name", "tag2":"name"})

    arguments(sys.argv[1:])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
