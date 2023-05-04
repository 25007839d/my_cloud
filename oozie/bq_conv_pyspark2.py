import requests
import sys
from pyspark.sql.functions import concat, udf, col, struct, lit, first, length, concat_ws, array, countDistinct, when, from_unixtime\
,split, sum, count, substring, translate, coalesce, regexp_replace, unix_timestamp
from datetime import date, datetime, timedelta
from os.path import abspath
from pyspark.sql import SparkSession
#from vzmi.vznet.sparkcommon.kafka.kafkautils import KafkaUtils
from common_functions import *

from pyspark.sql.types import *
import logging

warehouse_location = abspath('spark-warehouse')
def main():
    ref_df = spark.read.option("header",True).csv(radio_decoder_input_path)

    ref_df1 = ref_df.select(col('Bands supported').alias('band_capable'), col('MIMO ').alias('configuration'), col('DL Freq (MHz)').alias('frequency_capable'), col('Name - temp field').alias('model'), col('productCode').alias('product_code'), col('4Rx Capable').alias('rx4_capable'), col('4Tx Capable?').alias('tx4_capable'), col('Site Type').alias('site_type'), col('Vendor').alias('vendor'), col('Dual-Band?').alias('dual_band_capable'), col('NR Capable').alias('nr_capable'))

    radio_capability_df = ref_df1.withColumn("rx4_capable", when(ref_df1["rx4_capable"] == 'Y', "true").when(ref_df1["rx4_capable"] == 'N', "false").otherwise(None))\
                     .withColumn("tx4_capable", when(ref_df1["tx4_capable"] == 'Y', "true").when(ref_df1["tx4_capable"] == 'N', "false").otherwise(None))\
                     .withColumn("nr_capable", when(ref_df1["nr_capable"] == 'Y', "true").when(ref_df1["nr_capable"] == 'N', "false").otherwise(None))\
                     .withColumn("dual_band_capable", when(ref_df1["dual_band_capable"] == 'Y', "true").when(ref_df1["dual_band_capable"] == 'N', "false").otherwise(None))


    logger.info('radio_capability_df Count: %d', radio_capability_df.count())
    publish_to_hdfs(radio_capability_df)

def publish_to_hdfs(radio_capability_df):
    dimensions_dict = {"environment" : properties["env"], "dataset": "reference_radio_capability" }
    custom_metrics_dict = {}

    VzwRanDimReferenceRadioCapabilityLookupPreproc_table = properties["hive_db"] + "." + properties["VzwRanDimReferenceRadioCapabilityLookupPreproc_table"]
    VzwRanDimReferenceRadioCapabilityLookupPreproc_valid = properties["aidpe_core_dataset_root"] + "/" + properties["VzwRanDimReferenceRadioCapabilityLookupPreproc_dataset"] + "/" + "trans_dt=" + trans_dt
    VzwRanDimReferenceRadioCapabilityLookupPreproc_invalid = properties["aidpe_quarantine_root"] + "/" +properties["VzwRanDimReferenceRadioCapabilityLookupPreproc_dataset"] + "/" + "trans_dt=" + trans_dt
    radio_capability_df_src = properties["VzwRanDimReferenceRadioCapabilityLookupPreproc_dataset"]

    VzwRanDimReferenceRadioCapabilityLookupPreproc_valid_daily = properties["aidpe_core_dataset_daily_root"] + "/" + properties["VzwRanDimReferenceRadioCapabilityLookupPreproc_dataset"] + "/" + "trans_dt=" + trans_dt
    bootstrap_enabled = properties["bootstrap_enabled"]
    reconciliation_days = properties["reconciliation_days"]

    logger.info("Target table: {}".format(VzwRanDimReferenceRadioCapabilityLookupPreproc_table))
    logger.info("Target Core data Path: {}".format(VzwRanDimReferenceRadioCapabilityLookupPreproc_valid))
    logger.info("Target Quarantine Path: {}".format(VzwRanDimReferenceRadioCapabilityLookupPreproc_invalid))

    #publishResults(radio_capability_df, VzwRanDimReferenceRadioCapabilityLookupPreproc_table, VzwRanDimReferenceRadioCapabilityLookupPreproc_valid, VzwRanDimReferenceRadioCapabilityLookupPreproc_invalid, spark, properties, input_dir, trans_dt, logger, dimensions_dict, custom_metrics_dict, radio_capability_df_src)
    publish_reconciled_results(radio_capability_df, VzwRanDimReferenceRadioCapabilityLookupPreproc_table, VzwRanDimReferenceRadioCapabilityLookupPreproc_valid, VzwRanDimReferenceRadioCapabilityLookupPreproc_invalid, spark, properties, input_dir, trans_dt, logger, dimensions_dict, custom_metrics_dict, radio_capability_df_src, VzwRanDimReferenceRadioCapabilityLookupPreproc_valid_daily,reconciliation_days, bootstrap_enabled)

    logger.info('complete')


if __name__ == '__main__':
    #Initialize Logger
    logger = logging.getLogger('digital_twin_atoll_antenna')
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    properties = {}
    # Read in command line arguments
    for arg in sys.argv[1:]:
        try:
            key, value = arg.split('=', 1)
            properties[key] = value
        except IndexError as err:
            logging.exception('Index not found', err)
        except ValueError as err:
            logging.exception('Error parsing properties file for arg = ' + str(arg), err)

    logging.info('Command line arguments read')

    base_app_name = properties['base_app_name']
    env = properties['env']
    app_name = base_app_name + '_' + env
    logger.info('Running for env: %s', env)

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

    spark_context = spark.sparkContext

    properties = {}

    # Read in command line arguments
    for arg in sys.argv[1:]:
        try:
            key, value = arg.split('=', 1)
            properties[key] = value
        except IndexError as err:
            logging.exception('Index not found', err)
        except ValueError as err:
            logging.exception('Error parsing properties file for arg = ' + str(arg), err)


    trans_dt = properties["trans_dt"]
    radio_decoder_input_path = properties["radio_decoder_input_path"]

    # Yamas metrics details error alerts
    dimensions_dict_alert = {"environment" : properties['env'], "alert": "reference_health", "dataset": "reference_radio", "trans_dt": trans_dt}
    metrics_dict_alert = {"dataProcessingError.count": 1}
    dt = datetime.strptime(trans_dt, "%Y-%m-%d")

    try:
        main()
    except Exception as e:
        print(e)
        print("Error while running the spark job. Gracefully existing and backfilling the partition using historical data")
        publish_to_hdfs(None)
        publish_to_cloud_logging(dimensions_dict_alert, metrics_dict_alert, 'dataProcessingError')
#        post_yamas_request("Spark", properties['yamas_url'], properties['yamas_namespace'], dt, dimensions_dict_alert, metrics_dict_alert, logger)
