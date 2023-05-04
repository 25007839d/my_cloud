import sys, os
from subprocess import Popen, PIPE, STDOUT, check_output, CalledProcessError
import re
import logging
import json
import requests
from datetime import datetime, timedelta
from google.cloud import logging as gcloud_log
import uuid
from airflow.models import Variable


# LOGGER settings
LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s", datefmt='%Y.%m.%d %H:%M:%S')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
logging.root.setLevel(logging.INFO)

# REGEX PATTERN
TRANS_DT_PATTERN = r'\d{4}-\d{2}-\d{2}'

# Convert Date to Epoch Time
def epoch_seconds(dt):
    EPOCH = datetime.utcfromtimestamp(0)
    return int((dt - EPOCH).total_seconds())


# Post Yamas Request
# def post_yamas_request(app_name, yamas_url, namespace, dt, dimensions_dict, metrics_dict, logger):
#     #yamas_url_with_namespace="https://collector.yms.ops.yahoo.com:4443/yms/V2/sendMessage?namespace=vznet_dev"
#
#     attempts = 0
#     yamas_url_with_namespace = yamas_url +"?namespace=" + namespace
#     epoch_timestamp = epoch_seconds(dt)
#     yamas_data_dict = {"application": app_name, "timestamp": epoch_timestamp, "dimensions": dimensions_dict, "metrics": metrics_dict}
#     while attempts < 3:
#         try:
#             response = requests.post(yamas_url_with_namespace, data = json.dumps(yamas_data_dict) , headers={"Content-Type":"application/json"})
#             logger.info(response.text)
#             if response.status_code != 200:
#                 response.raise_for_status()
#             return
#         except requests.exceptions.RequestException as err:
#             logger.error(err)
#             attempts += 1
#             logger.info("Attempt number for Yamas push: %s", attempts)
#     logger.info("Aborting Yamas push due to %s attemps", attempts)

def publish_to_cloud_logging(dimensions_dict, metrics_dict, logger_name):
        """Writes log entries to the given logger."""
        logging_client = gcloud_log.Client()

        # This log can be found in the Cloud Logging console under 'Custom Logs'.
        gcloud_logger = logging_client.logger(logger_name)
        uniqueId = {'uniqueId': uuid.uuid4().hex}
        merge_dict = {**uniqueId, **dimensions_dict, **metrics_dict}
        print(merge_dict)
        # Struct log. The struct can be any JSON-serializable dictionary.
        gcloud_logger.log_struct(
            merge_dict
        )

        print("Wrote logs to {}.".format(logger_name))

# Function to run commands
def run_command(args_list):
    LOGGER.info("Running command: {}".format(str(args_list)))
    cmd_dict = {}
    out = None
    try:
        out = check_output(args_list,stderr=STDOUT, shell=True)
    except CalledProcessError as exc:
        cmd_dict["returncode"] = exc.returncode
        cmd_dict["pipe"] = exc.output.decode('UTF-8')
    else:
        cmd_dict["returncode"] = True
        cmd_dict["pipe"] = out.decode('UTF-8')
    return cmd_dict

# function for commonly used test -e command
def test_existence(check_path):
    returncode = run_command(' '.join(["hadoop", "fs", "-test", "-e", check_path]))["returncode"]
    return returncode

def get_trans_dt_from_lof(lof_output_file_location, market_name):

    # check if LOF exists
    lof_exists = test_existence(lof_output_file_location)
    if not lof_exists:
        print("LOF does not exist")
        return 0

    result_out = run_command(' '.join(['hadoop','fs', '-cat', lof_output_file_location]))
    print (result_out)
    result = result_out["pipe"]
    print (result)
    source_tar_paths = result.splitlines()
    market_tar_lst = []
    # Searching for all instances of the market tar gz in LOF output
    for tar_path in source_tar_paths:
        region_matches = re.search("nokia."+ market_name + ".latest.xcm.gz", tar_path)
        if region_matches:
          LOGGER.info("ADDING TAR PATH: {}".format(tar_path))
          market_tar_lst.append(tar_path)

    # If one or more paths were found
    if len(market_tar_lst) >= 1:
        #Sorting list to pick the latest file for the market
        market_tar_lst.sort(reverse=True)
        return market_tar_lst[0]
    else:
#        post_yamas_request("Spark", yamas_url, yamas_namespace, dt, dimensions_dict_alert, metrics_dict_alert, LOGGER)
        publish_to_cloud_logging(dimensions_dict_alert, metrics_dict_alert, 'missingMarket')
        return 0

if len(sys.argv) < 2:
    print("No Arguments.")
    exit(1)

if __name__ == "__main__":
    lof_output_file_location = str(sys.argv[1])
    market_name = str(sys.argv[2])
    schedule_dt = str(sys.argv[3])
    env = str(sys.argv[4])
    vendor = str(sys.argv[5])

    dt = datetime.strptime(schedule_dt, "%Y-%m-%d")
    dimensions_dict_alert = {"environment" : env, "vendor": vendor, "alert": "market_health", "market_name": market_name, "trans_dt": schedule_dt}
    metrics_dict_alert = {"missingMarket.count": 1}


    file_path = get_trans_dt_from_lof(lof_output_file_location, market_name)


    if(file_path == 0):
        print("file_path={}".format(file_path))
        print("trans_dt={}".format(schedule_dt))
        print("market_exists={}".format("false"))
        Variable.set("file_path", file_path)  # add path to variable env
        Variable.set("trans_dt", schedule_dt)  # add path to variable env
        Variable.set("market_exists", "false")  # add path to variable env
    else:
        LOGGER.info("FILE PATH FROM LOF: {}".format(file_path))
        trans_dt = re.search(TRANS_DT_PATTERN, file_path).group(0)
        print("file_path={}".format(file_path))
        print("trans_dt={}".format(trans_dt))
        print("market_exists={}".format("true"))
        Variable.set("file_path", file_path)  # add path to variable env
        Variable.set("trans_dt", trans_dt)  # add path to variable env
        Variable.set("market_exists", "true")  # add path to variable env