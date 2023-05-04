
import json
import logging
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
from pyspark.sql import Window
from datetime import date, datetime, timedelta
import pyspark.sql.functions as f
from google.cloud import logging as gcloud_log
import uuid

# define Constant
EPOCH = datetime.utcfromtimestamp(0)

# Keys to be used in the window function for reconciliation process
keys_map={}
dim_inventory_antenna_devices_norm_v0_keys=["sector", "carrier", "device_uid", "enodeb_id","centerline","band_enabled"]
dim_inventory_bbu_devices_norm_v0_keys=["serial_number"]
dim_inventory_cell_sites_norm_v0_keys=["site_id"]
dim_inventory_enodeb_norm_v0_keys=["enodeb_id"]
dim_inventory_enodeb_site_map_norm_v0_keys=["alternate_site_id","alternate_system","carrier","enodeb_id","gnb_du_id","sector","site_id"]
dim_inventory_project_norm_v0_keys=["project_id", "site_id"]
#Adjust the keys based on Jay's inputs
#dim_inventory_radio_bbu_antenna_assignment_norm_v0_keys=["antenna_uid", "bbu_uid", "carrier", "enodeb_id", "gnb_du_id", "gnb_fsu_id", "radio_uid", "sector"]
dim_inventory_radio_bbu_antenna_assignment_norm_v0_keys=["carrier", "enodeb_id", "gnb_du_id", "gnb_fsu_id", "sector"]
dim_inventory_radio_bbu_antenna_unassigned_norm_v0_keys=["antenna_uid", "bbu_uid", "carrier", "enodeb_id", "gnb_du_id", "gnb_fsu_id", "radio_uid", "sector"]
dim_inventory_radio_cell_devices_norm_v0_keys=["carrier", "device_uid", "enodeb_id", "sector"]
dim_inventory_radio_cell_unassigned_norm_v0_keys=["carrier", "device_uid", "enodeb_id", "sector"]
dim_inventory_radio_devices_norm_v0_keys=["device_uid"]
dim_inventory_radio_unassigned_norm_v0_keys=["device_uid"]
dim_reference_radio_capability_lookup_preproc_v0_keys=["product_code"]
dim_performance_energy_norm_v0_keys=["year","month","property_id","vendor_name"]

keys_map["dim_inventory_antenna_devices_norm_v0"] = dim_inventory_antenna_devices_norm_v0_keys
keys_map["dim_inventory_bbu_devices_norm_v0"]=dim_inventory_bbu_devices_norm_v0_keys
keys_map["dim_inventory_cell_sites_norm_v0"]=dim_inventory_cell_sites_norm_v0_keys
keys_map["dim_inventory_enodeb_norm_v0"]=dim_inventory_enodeb_norm_v0_keys
keys_map["dim_inventory_enodeb_site_map_norm_v0"]=dim_inventory_enodeb_site_map_norm_v0_keys
keys_map["dim_inventory_project_norm_v0"]=dim_inventory_project_norm_v0_keys
keys_map["dim_inventory_radio_bbu_antenna_assignment_norm_v0"]=dim_inventory_radio_bbu_antenna_assignment_norm_v0_keys
keys_map["dim_inventory_radio_bbu_antenna_unassigned_norm_v0"]=dim_inventory_radio_bbu_antenna_unassigned_norm_v0_keys
keys_map["dim_inventory_radio_cell_devices_norm_v0"]=dim_inventory_radio_cell_devices_norm_v0_keys
keys_map["dim_inventory_radio_cell_unassigned_norm_v0"]=dim_inventory_radio_cell_unassigned_norm_v0_keys
keys_map["dim_inventory_radio_devices_norm_v0"]=dim_inventory_radio_devices_norm_v0_keys
keys_map["dim_inventory_radio_unassigned_norm_v0"]=dim_inventory_radio_unassigned_norm_v0_keys
keys_map["dim_reference_radio_capability_lookup_preproc_v0"]=dim_reference_radio_capability_lookup_preproc_v0_keys
keys_map["dim_performance_energy_norm_v0"]=dim_performance_energy_norm_v0_keys


# Categories of tables based on their partition columns, will be used to find out the latest partition which has data
transdt_based_table_list=['dim_inventory_antenna_devices_norm_v0','dim_inventory_cell_sites_norm_v0','dim_inventory_enodeb_site_map_norm_v0','dim_inventory_project_norm_v0','dim_reference_radio_capability_lookup_preproc_v0']
switchx_partition_table_list=['dim_inventory_bbu_devices_norm_v0','dim_inventory_enodeb_norm_v0','dim_inventory_radio_bbu_antenna_assignment_norm_v0','dim_inventory_radio_bbu_antenna_unassigned_norm_v0','dim_inventory_radio_cell_devices_norm_v0','dim_inventory_radio_cell_unassigned_norm_v0','dim_inventory_radio_devices_norm_v0','dim_inventory_radio_unassigned_norm_v0']

# Map to derive column names for quarantine struct
quar_col_map = {}
quar_col_map["col_name"]="col_name"
quar_col_map["col_value"]="col_value"
quar_col_map["quarantine_reason"]="quarantine_reason"

QUARANTINE_INFO_STRUCT_SCHEMA = ArrayType(StructType([StructField(quar_col_map["col_name"], StringType(), nullable=True),StructField(quar_col_map["col_value"], StringType(), nullable=True),StructField(quar_col_map["quarantine_reason"], StringType(), nullable=True)]))
QUAR_ELEMENT_DELIM='#'
QUAR_REASON_DELIM='|'

def logger_setup(logger):
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

# Convert dt to epoch time
def epoch_seconds(dt):
    return int((dt - EPOCH).total_seconds())

def get_timestamp_from_trans_dt(trans_dt):
    return int(datetime.strptime(trans_dt, "%Y-%m-%d").timestamp())/ 1000

def add_epoch_timestamp():
    """compute the current timestamp in epoch milliseconds"""
    timestamp = int(time.time())/ 1000
    return timestamp

# def post_yamas_request(app_name, yamas_url, namespace, dt, dimensions_dict, metrics_dict, logger):
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

def get_avro_schema_url(spark, hivedb_with_table):
    df = spark.sql("show TBLPROPERTIES {}".format(hivedb_with_table))
    df.show()
    avro_schema_url = df.filter(col("key")=="avro.schema.url").select('value').collect()[0].value
    return avro_schema_url

def merge_metrics_dicts(metrics_dict, custom_metrics_dict):
    merged_metrics_dict = metrics_dict.copy()
    merged_metrics_dict.update(custom_metrics_dict)
    return merged_metrics_dict

def df_validation(spark_dataframe, hivedb_with_table, spark, input_dir, trans_dt, logger, src, properties):

    avro_schema_url = get_avro_schema_url(spark, hivedb_with_table)
    avro_schema = spark.read.json(avro_schema_url, multiLine=True)
    avsc_record_name = avro_schema.select("name").collect()[0].name
    logger.info("recordName from avsc: {}".format(avsc_record_name))

    data_present = True
    if spark_dataframe is None or spark_dataframe.count() == 0:
        data_present = False
        return None, None, avsc_record_name, data_present

    host = properties.get('host')
    partition_by_trans_dt_flg = properties.get('partition_by_trans_dt_flg')

    if host is None:
        host="switchx"

    timestamp = get_timestamp_from_trans_dt(trans_dt)
    event_ingress_ts = timestamp
    spark_dataframe.persist()

    listColumns=spark_dataframe.columns

    # create the output_record in the proper avro format
    if not 'timestamp' in spark_dataframe.columns:
        spark_dataframe = spark_dataframe.withColumn("timestamp", lit(timestamp))

    if not 'host' in spark_dataframe.columns:
        spark_dataframe = spark_dataframe.withColumn("host", lit(host))

    if not 'src' in spark_dataframe.columns:
        spark_dataframe = spark_dataframe.withColumn("src", lit(src))

    if not '_event_ingress_ts' in spark_dataframe.columns:
        spark_dataframe = spark_dataframe.withColumn("_event_ingress_ts", lit(event_ingress_ts))

    if not '_event_origin' in spark_dataframe.columns:
        spark_dataframe = spark_dataframe.withColumn("_event_origin", lit(input_dir))

    if not '_event_tags' in spark_dataframe.columns:
        spark_dataframe = spark_dataframe.withColumn("_event_tags", array([]))

    spark_dataframe = validateDataFrame(spark_dataframe, avro_schema, partition_by_trans_dt_flg)
    logger.info("Total Entries from validateDataFrame: {}".format(spark_dataframe.count()))

    spark_dataframe = spark_dataframe.withColumn("quarantine_information",f.array_except(col("reason_array"),array(struct(lit(None).cast("string").alias("col_name"),lit(None).cast("string").alias("col_value"),lit(None).cast("string").alias("quarantine_reason")))))
    spark_dataframe = spark_dataframe.drop("reason_array")

    valid_data = spark_dataframe.filter(f.size(col("quarantine_information")) == 1)
    valid_data = valid_data.drop("quarantine_information")

    quarantined_data = spark_dataframe.filter(f.size(col("quarantine_information")) > 1)
    quarantined_data.drop("quarantine_reason")

    logger.info("Schema for valid_data")
    valid_data.printSchema()
    isbm kallinga

    logger.info("Schema for quarantined_data")
    quarantined_data.printSchema()

    valid_data = valid_data.dropDuplicates()
    quarantined_data = quarantined_data.dropDuplicates()

    return valid_data, quarantined_data, avsc_record_name, data_present

def write_to_hdfs(dataset,dataset_path,partition_key):
    if partition_key is None:
        dataset.coalesce(1).write.format("avro").mode('overwrite').save(dataset_path)
    else:
        dataset.coalesce(1).write.partitionBy(partition_key).format("avro").mode('overwrite').save(dataset_path)

def write_to_hdfs_avro_recordname(dataset,dataset_path,avsc_record_name,partition_key):
    if partition_key is None:
        dataset.coalesce(1).write.format("avro").option("recordName", avsc_record_name).mode('overwrite').save(dataset_path)
    else:
        dataset.coalesce(1).write.partitionBy(partition_key).format("avro").option("recordName", avsc_record_name).mode('overwrite').save(dataset_path)

def push_yamas_metrics(hivedb_with_table,valid_data_count,quarantined_data_count,dimensions_dict, custom_metrics_dict,trans_dt,logger, properties):
    # Yamas metrics
    metrics_dict = {}
    metrics_dict[hivedb_with_table + ".spark.valid.count"] = valid_data_count
    metrics_dict[hivedb_with_table + ".spark.quarantine.count"] = quarantined_data_count

    if custom_metrics_dict != {}:
        metrics_dict = merge_metrics_dicts(metrics_dict, custom_metrics_dict)

    dt = datetime.strptime(trans_dt, "%Y-%m-%d")

    yamas_attempt = 0
    try:
        post_yamas_request("Spark", properties["yamas_url"], properties["yamas_namespace"], dt, dimensions_dict, metrics_dict, logger)
    except:
        yamas_attempt+=1
        if yamas_attempt < 3:
            post_yamas_request("Spark", properties["yamas_url"], properties["yamas_namespace"], dt, dimensions_dict, metrics_dict, logger)

def publishResults(spark_dataframe, hivedb_with_table, valid_dataset, invalid_dataset, spark, properties, input_dir, trans_dt, logger, dimensions_dict, custom_metrics_dict, src):

    enable_namespace_flg = properties.get('enable_namespace_flg')
    partition_by_trans_dt_flg = properties.get('partition_by_trans_dt_flg')
    print("enable_namespace_flg: "+str(enable_namespace_flg))
    print("partition_by_trans_dt_flg: "+str(partition_by_trans_dt_flg))

    valid_data, quarantined_data, avsc_record_name, data_present = df_validation(spark_dataframe, hivedb_with_table, spark, input_dir, trans_dt, logger, src, properties)

    if not data_present:
        print("Dataframe is null or empty, exiting")
        return

    valid_data_count = valid_data.count()
    quarantined_data_count = quarantined_data.count()

    partition_key = None
    if partition_by_trans_dt_flg is not None:
        print("partition_by_trans_dt_flg is not None")
        partition_key = "trans_dt"

    logger.info("Daily snapshot count of Valid records for {}: {}".format(hivedb_with_table, valid_data_count))
    if enable_namespace_flg is None:
        print("enable_namespace_flg is None")
        write_to_hdfs(valid_data, valid_dataset,partition_key)
    else:
        print("partition_by_trans_dt_flg is None")
        write_to_hdfs_avro_recordname(valid_data, valid_dataset,avsc_record_name,partition_key)

    logger.info("Daily snapshot count of Invalid records for {}: {}".format(hivedb_with_table, quarantined_data_count))
    write_to_hdfs_avro_recordname(quarantined_data, invalid_dataset,avsc_record_name,partition_key)

    #push_yamas_metrics(hivedb_with_table,valid_data_count,quarantined_data_count,dimensions_dict, custom_metrics_dict,trans_dt,logger, properties )


def publish_reconciled_results(spark_dataframe, hivedb_with_table, valid_dataset, invalid_dataset, spark, properties, input_dir, trans_dt, logger, dimensions_dict, custom_metrics_dict, src, daily_valid_dataset, reconciliation_days, bootstrap_enabled):

    logger.info("Printing the args:")
    logger.info("hivedb_with_table :{}".format(hivedb_with_table))
    logger.info("valid_dataset :{}".format(valid_dataset))
    logger.info("invalid_dataset :{}".format(invalid_dataset))
    logger.info("input_dir :{}".format(input_dir))
    logger.info("trans_dt :{}".format(trans_dt))
    logger.info("src :{}".format(src))
    logger.info("daily_valid_dataset :{}".format(daily_valid_dataset))
    logger.info("reconciliation_days :{}".format(reconciliation_days))
    logger.info("bootstrap_enabled :{}".format(bootstrap_enabled))

    enable_namespace_flg = properties.get('enable_namespace_flg')
    partition_by_trans_dt_flg = properties.get('partition_by_trans_dt_flg')

    partition_key = None
    if partition_by_trans_dt_flg is not None:
        print("partition_by_trans_dt_flg is not None")
        partition_key = "trans_dt"

    valid_data, quarantined_data, avsc_record_name, data_present = df_validation(spark_dataframe, hivedb_with_table, spark, input_dir, trans_dt, logger, src, properties)

    if not data_present:
        print("Today's dataframe is null or empty, reconciliation will carry over the historical data in latest partition..")
        quarantined_data_count = 0

    if data_present:
        valid_data_count = valid_data.count()
        quarantined_data_count = quarantined_data.count()

        logger.info("Daily snapshot count of Valid records for {}: {}".format(hivedb_with_table, valid_data_count))
        logger.info("Writing Daily snapshot of Valid records for {}".format(hivedb_with_table))
        if enable_namespace_flg is None:
            write_to_hdfs(valid_data, daily_valid_dataset,partition_key)
        else:
            write_to_hdfs_avro_recordname(valid_data, daily_valid_dataset,avsc_record_name,partition_key)

        logger.info("Daily snapshot count of Invalid records for {}: {}".format(hivedb_with_table, quarantined_data_count))
        logger.info("Writing Daily snapshot of Invalid records for {}".format(hivedb_with_table))
        write_to_hdfs_avro_recordname(quarantined_data, invalid_dataset,avsc_record_name,partition_key)

    # Reconciliation logic
    valid_data = reconciliation(spark,properties,logger,valid_data,hivedb_with_table,trans_dt, bootstrap_enabled, reconciliation_days)

    if (valid_data is None):
        return
    valid_data_count = valid_data.count()

    logger.info("Writing reconciled dataset of Valid records for {}".format(hivedb_with_table))
    if enable_namespace_flg is None:
        write_to_hdfs(valid_data, valid_dataset,partition_key)
    else:
        write_to_hdfs_avro_recordname(valid_data, valid_dataset,avsc_record_name,partition_key)

    #push_yamas_metrics(hivedb_with_table,valid_data_count,quarantined_data_count,dimensions_dict, custom_metrics_dict,trans_dt,logger, properties )



def validateDataFrame(to_validate, avro_schema, partition_by_trans_dt_flg):
    avro_schema = avro_schema.withColumn("fields", explode(col("fields")))
    fieldsToCheck = avro_schema.select("fields.*")
    #the following messy null check is due to the mixed schema on the type column, should be replaced with something better
    fieldsToCheck = fieldsToCheck.withColumn("isNullable", col("type").contains("null"))
    localFieldList = fieldsToCheck.collect()
    validated_df = to_validate
    validated_df.persist()

    avscColumnList = []

    for field in localFieldList:

        typeFound = None
        if '''"type":"long"''' in field.type or ('''"type":"array"''' not in field.type and "long" in field.type):
            typeFound = "long"
        if '''"type":"string"''' in field.type or ('''"type":"array"''' not in field.type and "string" in field.type):
            typeFound = "string"
        if '''"type":"boolean"''' in field.type or ('''"type":"array"''' not in field.type and "boolean" in field.type):
            typeFound = "boolean"
        if '''"type":"int"''' in field.type or ('''"type":"array"''' not in field.type and "int" in field.type):
            typeFound = "int"
        if '''"type":"double"''' in field.type or ('''"type":"array"''' not in field.type and "double" in field.type):
            typeFound = "double"
        if '''"logicalType":"timestamp-millis"''' in field.type:
            typeFound = "timestamp"
      --  if not field["name"] in validated_df.columns: #making none instead of using default
       --     validated_df = validated_df.withColumn(field["name"], lit(None))
        # column previous df mai nahi hai to add kiya column
        if not field["isNullable"]: # all fields check
            validated_df = validated_df.withColumn("quarantine_null_"+ field["name"],
            when((validated_df[field["name"]].isNull()) |
            (trim(validated_df[field["name"]].cast("string")) == ""),
            struct(lit(field["name"]).alias("col_name"),validated_df[field["name"]].
             cast('string').alias("col_value"),lit("column can not be null or blank").
                   alias("quarantine_reason"))).otherwise(lit(None)))

        if typeFound is not None: # only array column not check
            # validated_df = validated_df.withColumn(field["name"]+"_original", col(field["name"]))
            validated_df = validated_df.withColumn(field["name"], col(field["name"]).cast(typeFound))
            validated_df = validated_df.withColumn("quarantine_cast_"+ field["name"],
                          when((((validated_df[field["name"]].isNotNull()) & (trim(validated_df[field["name"]]
                          .cast("string"))!= "")) & ((validated_df[field["name"]+"_original"].isNull()) |

                           (trim(validated_df[field["name"]+"_original"].cast("string")) == ""))) |

                            (validated_df[field["name"]].isNull() & ((validated_df[field["name"]+"_original"]
                            .isNotNull()) & (trim(validated_df[field["name"]+"_original"].cast("string")) != "")))

                            ,struct(lit(field["name"]).alias("col_name"),validated_df[field["name"]+"_original"]
                             .cast('string').alias("col_value"),concat(lit("could not cast to "),lit(typeFound)).
                              alias("quarantine_reason"))).otherwise(lit(None)))
        avscColumnList.append(field["name"])

    quar_columns = [column for column in validated_df.columns if column.startswith("quarantine_")]
    validated_df = validated_df.withColumn("reason_array",array(quar_columns))
    avscColumnList.append("reason_array")
    if partition_by_trans_dt_flg is not None:
        avscColumnList.append("trans_dt")
    final_df = validated_df.select(avscColumnList)
    return final_df

def string_list_to_dict_array(array_str):
    array_str_list = array_str.split(QUAR_REASON_DELIM)
    data_array = []

    for i in array_str_list:
        col_list = i.split(QUAR_ELEMENT_DELIM)
        data_map = {}
        data_map[quar_col_map["col_name"]]=col_list[0]
        data_map[quar_col_map["col_value"]]=col_list[1]
        data_map[quar_col_map["quarantine_reason"]]=col_list[2]
        data_array.append(data_map)

    return data_array

# Union 2 dataframes which may have mismatched columns
def customUnion(df1, df2):
    cols1 = df1.columns
    cols2 = df2.columns
    total_cols = sorted(cols1 + list(set(cols2) - set(cols1)))
    def expr(mycols, allcols):
        def processCols(colname):
            if colname in mycols:
                return colname
            else:
                return lit(None).alias(colname)
        cols = map(processCols, allcols)
        return list(cols)
    appended = df1.select(expr(cols1, total_cols)).union(df2.select(expr(cols2, total_cols)))
    return appended

# Get the latest row based on a key and date
def reconcile_df(input_df, date_column, key_list):
    windowSpec = Window.partitionBy(key_list).orderBy(col(date_column).desc())
    ranked_df = input_df.withColumn("dense_rank", dense_rank().over(windowSpec))
    master_data = ranked_df.filter(col("dense_rank") == 1)
    master_data = master_data.drop("dense_rank")
    return master_data

# Define udf
reason_array_struct = udf(lambda str: string_list_to_dict_array(str), QUARANTINE_INFO_STRUCT_SCHEMA)

def reconciliation(spark,properties,logger,valid_data,hivedb_with_table,trans_dt, bootstrap_enabled, reconciliation_days):

    if reconciliation_days is None:
        raise missingReconciliationInfoError(table)
    print("reconciliation_days: "+reconciliation_days)

    if bootstrap_enabled.lower() == 'true':
        bootstrap_enabled = True
    else:
        bootstrap_enabled = False

    table = hivedb_with_table.split(".")[1]
    keys_list = keys_map.get(table,[])
    if not keys_list:
        raise emptyKeyError(table)

    if table in switchx_partition_table_list:
        print("Processing a switchx based table")
        internal_vendor=properties['vendor']
        internal_market=properties['marketName']
        internal_technology=properties['technology']
        if (internal_vendor is None or internal_market is None or internal_technology is None):
            raise missingPartitionInfoError(table)
        filter_condition = "internal_vendor='{}' and internal_market='{}' and internal_technology='{}'".format(internal_vendor,internal_market,internal_technology)
    else:
        filter_condition = '1=1'

    logger.info("Checking bootstrap_enabled: {}".format(bootstrap_enabled))
    if bootstrap_enabled:
        print("Bootstrap run, pulling the historical data based on reconciliation_days")
        historical_date = datetime.strptime(trans_dt, '%Y-%m-%d') - timedelta(days=int(reconciliation_days))
        print("historical_date:"+str(historical_date))
        historical_data = spark.sql("SELECT * FROM {} where trans_dt>='{}' and trans_dt<'{}' and {}".format(hivedb_with_table, historical_date,trans_dt, filter_condition))
        logger.info("historical_data count: {}".format(historical_data.count()))
        historical_data_count = historical_data.count()
        if historical_data_count == 0:
            logger.info("historical_data not found.")
    else:
        print("Regular day run, pulling the history from latest available partition")
        # Using 14 days to pull stats for latest available partition
        historical_date = datetime.strptime(trans_dt, '%Y-%m-%d') - timedelta(days=int(14))
        latest_trans_dt_with_data = spark.sql('''
 SELECT MAX(trans_dt) as max_dt from (Select trans_dt, count(1) from {} where trans_dt >= '{}' and trans_dt<'{}' and {} group by trans_dt having count(1) > 0 )x'''.format(hivedb_with_table,historical_date,trans_dt,filter_condition)).rdd.map(lambda x: x.max_dt).collect()[0]
        print("latest_trans_dt_with_data:"+str(latest_trans_dt_with_data))
        historical_data = spark.sql("SELECT * FROM {} where trans_dt='{}' and {}".format(hivedb_with_table,latest_trans_dt_with_data, filter_condition))
        historical_data_count = historical_data.count()
        logger.info("historical_data count before applying reconciliation_days: {}".format(historical_data_count))
        if (not bootstrap_enabled) and (historical_data_count == 0):
            logger.info("historical_data not found.")

        if (historical_data_count > 0):
            #reconciliation_date is number of days from the last available trans_dt which needs to be considered for reconciliation. This will apply a rolling window logic on each run to reconcile the data only for last x days
            reconciliation_date = datetime.strptime(latest_trans_dt_with_data, '%Y-%m-%d') - timedelta(days=int(reconciliation_days))
            print("reconciliation_date:"+str(reconciliation_date))
            print("Filtering data to pick data between reconciliation_date and trans_dt")
            historical_data = historical_data.filter((col("_event_ingress_ts") >= reconciliation_date) & (col("_event_ingress_ts") < lit(trans_dt)))
            logger.info("historical_data count after applying reconciliation_days: {}".format(historical_data.count()))

    if (historical_data_count > 0):
        historical_data = historical_data.withColumn("_event_ingress_ts",((col("timestamp").cast("long"))/1000).cast("timestamp")).withColumn("timestamp",((col("timestamp").cast("long"))/1000).cast("timestamp"))

        if table in switchx_partition_table_list:
            historical_data = historical_data.drop("internal_vendor", "internal_market", "internal_technology")

    logger.info("Union daily snapshot and historical data.")
    #logger.info("Sample valid_data:")
    #valid_data.show(10,False)

    #logger.info("Sample historical_data:")
    #historical_data.show(10,False)

    if (valid_data is None or valid_data.count() == 0) and (historical_data is not None):
        valid_data = historical_data
    elif (valid_data is not None) and (historical_data is not None):
        valid_data = customUnion(valid_data, historical_data)
    valid_data = valid_data.dropDuplicates()
    # Repartition on the keys which will be used in window function for performance improvement
    valid_data = valid_data.repartition(*[col(c) for c in keys_list]).cache()
    valid_data_count = valid_data.count()
    logger.info("Valid record counts before reconciliation count for {}: {}".format(hivedb_with_table, valid_data_count))

    # Reconcile the data
    valid_data = reconcile_df(valid_data, "_event_ingress_ts", keys_list)
    valid_data = valid_data.dropDuplicates()
    valid_data_count = valid_data.count()
    logger.info("Valid record counts after reconciliation count for {}: {}".format(hivedb_with_table, valid_data_count))

    return valid_data


class emptyKeyError(Exception):
    def __init__(self, table):
        message = f"Set keys in keys_map for table {table}"
        super().__init__(message)

class missingReconciliationInfoError(Exception):
    def __init__(self, table):
        message = f"reconciliation_days is not provided for {table}"
        super().__init__(message)


class missingPartitionInfoError(Exception):
    def __init__(self, table):
        message = f"vendor,marketName or technology is not provided for {table}, they are missing in properties"
        super().__init__(message)

class missingMasterData(Exception):
    def __init__(self, table):
        message = f"Master data not found for {table}, please check why there is no data in the latest partition"
        super().__init__(message)

