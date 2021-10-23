# Viessmann_API_InfluxDB_Logger

This is a simple data logger for Viessmann API - Tested with 'Vitodens 300'

## Useful Links:
- https://developer.viessmann.com/ [API-Doc]
- https://www.viessmann-community.com/ [Forum]


## Install and Requirements
1. Install Python3 and pip3
2. Run: `python3 -m venv venv` to create venv
3. Activate Python-venv
   1. Linux: `source venv/bin/activate`
   2. Windows: `venv/bin/activate.ps1` [no tested]
4. Install Python-Requirements `pip3 install -r requirements.txt`
5. Configure 'config' files (in configs/):
   1. `configs/influx_db.json`
      1. Copy the file `configs/influx_db.json.sample` to `configs/influx_db.json`
      2. Edit the file according to you influx-db-server-settings
   2. `configs/viessmann_api.json`
      1. Copy the file `configs/viessmann_api.json.sample` to `configs/viessmann_api.json`
      2. Edit the credentials according to: https://developer.viessmann.com/
         1. Go to: https://developer.viessmann.com/
         2. Create a new API-Key (disable reCAPTCHA)
         3. Copy: 'Client_ID' to 'client_id' (in `config/viessmann_api.json`)
         4. Create a new 'Code_verifier' using: https://tonyxu-io.github.io/pkce-generator/
         5. Copy: 'Code_verifier' to 'code_challenge' (in `config/viessmann_api.json`)
6. Run the APP:
   1. `python3 python_viessmannapi.py`
      1. Fetch all other data by starting the app follow the steps 1-7
   2. `python3 python_viessmannapi.py -h` to list commands
   3. `python3 python_viessmannapi.py -r auto` to run one single query(API) and write to db

## Additional
   - Logging: A log file will be created at: `./log_file.log`