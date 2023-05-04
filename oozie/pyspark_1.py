"""
Copyright (c) 2020. Verizon Media
"""

import logging
import sys
from datetime import date, datetime, timedelta
from os.path import abspath
from pyspark.sql.functions import concat, udf, col, struct, trim, lpad, split, lit, first, length, concat_ws, array, countDistinct, when, \
    from_unixtime \
    , split, sum, count, substring, translate, coalesce, regexp_replace, regexp_extract, unix_timestamp, md5
from pyspark.sql import SparkSession
from pyspark.sql.types import *


from common_functions import *

def main():

    ########################################################################################################################
    # ETL the Atoll Data
    ########################################################################################################################
    from pyspark.sql import SparkSession

    spark = SparkSession \
        .builder \
        .master('yarn') \
        .appName('spark-bigquery-demo') \
        .getOrCreate()

    # Use the Cloud Storage bucket for temporary BigQuery export data used
    # by the connector.
    bucket = "[bucket]"
    spark.conf.set('temporaryGcsBucket', bucket)

    # Load data from BigQuery.
    words = spark.read.format('bigquery') \
        .option('table', 'bigquery-public-data:samples.shakespeare') \
        .load()
    words.createOrReplaceTempView('words')
    logger.info('Load static reference data from OC table')

    antenna_atoll_df = spark.sql('''select gnb_enb_id,sector_number,carrier_number,antenna_model,antenna_manufacturer,
                                 number_of_transmission_antenna_ports,ret_antenna_model,antenna_type,band_info,antenna_frequency_mhz,centerline_ft,
                                 number_of_reception_antenna_ports,radio_access_technology,azimuth_deg,gnb_du_id from vzn_ndl_vzatoll_core_tbls.lte_topology_raw_v4
                                 where gnb_enb_vendor IN ('Nokia', 'Samsung','Ericsson') and site_version='0000' and gnb_enb_id!='' and sector_number!='' and carrier_number!=''
                                 and (radio_access_technology='LTE' OR radio_access_technology='5GNR') and trans_dt="''' + query_date +'"').persist()
    antenna_atoll_df = antenna_atoll_df.withColumn('band_info_1', split(antenna_atoll_df['band_info'], '_').getItem(0)) \
        .withColumn('band_info_2', split(antenna_atoll_df['band_info'], '_').getItem(1)) \
        .withColumn('band_earfcn', split(antenna_atoll_df['band_info'], '_').getItem(2))

    antenna_atoll_df = antenna_atoll_df.withColumn('carrier_type', when(col('band_info_1') == 'B1', 'MB') \
                                                   .when(col('band_info_1') == 'B13', 'LB') \
                                                   .when(col('band_info_1') == 'B2', 'MB') \
                                                   .when(col('band_info_1') == 'B252', 'LAA') \
                                                   .when(col('band_info_1') == 'B4', 'MB') \
                                                   .when(col('band_info_1') == 'B46', 'LAA') \
                                                   .when(col('band_info_1') == 'B48', 'CBRS') \
                                                   .when(col('band_info_1') == 'B5', 'LB') \
                                                   .when(col('band_info_1') == 'B66', 'MB') \
                                                   .when(col('band_info_1') == 'Bn2', 'MB') \
                                                   .when(col('band_info_1') == 'Bn260', 'HB') \
                                                   .when(col('band_info_1') == 'Bn261', 'HB') \
                                                   .when(col('band_info_1') == 'Bn5', 'LB') \
                                                   .when(col('band_info_1') == 'Bn66', 'MB') \
                                                   .when(col('band_info_1') == 'BnLS6', 'MB').otherwise(''))
    antenna_atoll_df = antenna_atoll_df.withColumn("antenna_uid", when((col('number_of_reception_antenna_ports') == '2') & (col('carrier_type') == 'LB'), concat_ws('_', col('gnb_enb_id'), col('sector_number'), col('band_earfcn'), col('antenna_model'), col('azimuth_deg'), col('centerline_ft')))
                                                   .when((col('number_of_reception_antenna_ports') == '4') & (col('carrier_type') == 'LB'), '')
                                                   .when((col('number_of_reception_antenna_ports') == '2') & (col('carrier_type') == 'MB'), concat_ws('_', col('gnb_enb_id'), col('sector_number'), col('band_earfcn'), col('antenna_model'), col('azimuth_deg'), col('centerline_ft')))
                                                   .when((col('number_of_reception_antenna_ports') == '4') & (col('carrier_type') == 'MB'), concat_ws('_', col('gnb_enb_id'), col('sector_number'), col('band_earfcn'), col('antenna_model'), col('azimuth_deg')))
                                                   .when(col('carrier_type') == 'LAA', concat_ws('_', col('gnb_enb_id'), col('sector_number'), col('carrier_type'), col('antenna_model'), col('azimuth_deg')))
                                                   .when(col('carrier_type') == 'CBRS', concat_ws('_', col('gnb_enb_id'), col('sector_number'), col('carrier_type'), col('antenna_model'), col('azimuth_deg')))
                                                   .when(col('carrier_type') == 'HB', concat_ws('_', col('gnb_enb_id'), col('sector_number'), col('carrier_type'), col('antenna_model'), col('azimuth_deg')))
                                                   .when((col('number_of_reception_antenna_ports') == '1') | (col('number_of_reception_antenna_ports') == '16') | (col('number_of_reception_antenna_ports') == '8') | (col('number_of_reception_antenna_ports') == '6'), concat_ws('_', col('gnb_enb_id'), col('sector_number'), col('band_earfcn'), col('antenna_model'), col('azimuth_deg')))
                                                   )
    antenna_atoll_df = antenna_atoll_df.filter(col('antenna_uid') != '')
    antenna_final_df = antenna_atoll_df.select(col('azimuth_deg').alias('azimuth'), col('band_info').alias('band_enabled'), col('carrier_number').alias('carrier'),
                                               col('carrier_type'), col('centerline_ft').alias('centerline'), col('antenna_uid').alias('device_uid'), col('gnb_enb_id').alias('enodeb_id'),
                                               col('antenna_frequency_mhz').alias('frequency_enabled'), col('antenna_model').alias('model'),
                                               col('number_of_reception_antenna_ports').alias('rx_port_count'), col('sector_number').alias('sector'),
                                               col('number_of_transmission_antenna_ports').alias('tx_port_count'),
                                               col('antenna_type').alias('type'), col('antenna_manufacturer').alias('vendor'), col('gnb_du_id').alias('gnb_du_id')
                                               ).distinct().persist()

    antenna_final_df = antenna_final_df.withColumn('uid_hash', md5('device_uid'))
    antenna_final_df = antenna_final_df.drop('device_uid').withColumnRenamed('uid_hash', 'device_uid')

    logger.info('antenna_final_df count: %d', antenna_final_df.count())
    publish_to_hdfs(antenna_final_df)

def publish_to_hdfs(antenna_final_df):
    dimensions_dict = {"environment" : properties["env"], "dataset": "reference_antenna" }
    custom_metrics_dict = {}
    AntennaDevicesNormTable = properties["hive_db"] + "." + properties["VzwRanDimInventoryAntennaDevicesNorm_table"]
    AntennaNorm_valid = properties["antenna_core_path"]
    AntennaDevicesNorm_invalid = properties["antenna_quarantine_path"]
    antenna_final_df_src = properties["VzwRanDimInventoryAntennaDevicesNorm_dataset"]

    # Reconciliation settings
    AntennaNorm_valid_daily = properties["antenna_core_path_daily"]
    reconciliation_days = properties["reconciliation_days"]
    bootstrap_enabled = properties["bootstrap_enabled"]

    publish_reconciled_results(antenna_final_df, AntennaDevicesNormTable, AntennaNorm_valid, AntennaDevicesNorm_invalid, spark, properties, input_dir, trans_dt, logger, dimensions_dict, custom_metrics_dict, antenna_final_df_src, AntennaNorm_valid_daily, reconciliation_days, bootstrap_enabled)

    logger.info('complete')


if __name__ == '__main__':
    ########################################################################################################################
    # Initial Setup
    ########################################################################################################################
    warehouse_location = abspath('spark-warehouse')

    ##Initialize Logger
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

    input_dir = "vzn_ndl_vzatoll_core_tbls.lte_topology_raw_v4"
    trans_dt = properties["trans_dt"]

    # NDL lands data every hour. Use current date as we run at nearly 9am UTC
    run_date = properties['run_date']
    logger.info('run_date: %s', run_date)
    query_date = datetime.strftime(datetime.strptime(run_date, '%Y%m%d'), '%Y-%m-%d')
    logger.info('query_date: %s', query_date)

    # Yamas metrics details error alerts
    dimensions_dict_alert = {"environment" : properties['env'], "alert": "reference_health", "dataset": "reference_antenna", "trans_dt": trans_dt}
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