from __future__ import absolute_import
import argparse
from datetime import datetime
import json
import logging
import os
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

from flatten_json import flatten
import uuid

# import json
# import pandas as pd
# import xmltodict

PROJECT_ID = 'cio-sea-team-lab-9e09db'
BUCKET = 'cio-sea-team-lab-9e09db-ecp-project'
JOB_NAME = 'file-json-with-xml-to-file'
datasetId_Staging = 'Pulkit_JSON'
Unique_id = (uuid.uuid1().time_low) + (uuid.uuid1().time)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cio-sea-team-lab-9e09db-0b2782fa290b.json'

table_schema = {
    'fields': [{"name": "pubsub_id", "type": "INTEGER", "mode": "NULLABLE"},
               {"name": "pubsub_ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
               {"name": "df_current_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
               {"name": "baseType", "type": "STRING", "mode": "NULLABLE"},
               {"name": "type", "type": "STRING", "mode": "NULLABLE"},
               {"name": "category", "type": "STRING", "mode": "REQUIRED"},
               {"name": "characteristicName", "type": "STRING", "mode": "NULLABLE"},
               {"name": "characteristicValue", "type": "STRING", "mode": "NULLABLE"},
               {"name": "characteristicValueType", "type": "STRING", "mode": "NULLABLE"},
               {"name": "content", "type": "STRING", "mode": "REQUIRED"},
               {"name": "href", "type": "STRING", "mode": "NULLABLE"},
               {"name": "id", "type": "STRING", "mode": "REQUIRED"},
               {"name": "language", "type": "STRING", "mode": "REQUIRED"},
               {"name": "logFlag", "type": "STRING", "mode": "NULLABLE"},
               {"name": "messageType", "type": "STRING", "mode": "REQUIRED"},
               {"name": "priority", "type": "INTEGER", "mode": "NULLABLE"},
               {"name": "receiverBaseType", "type": "STRING", "mode": "NULLABLE"},
               {"name": "receiverType", "type": "STRING", "mode": "NULLABLE"},
               {"name": "receiverBillingAccountMastersourceId", "type": "INTEGER", "mode": "NULLABLE"},
               {"name": "receiverBillingAccountNumber", "type": "INTEGER", "mode": "NULLABLE"},
               {"name": "receiverDeliveryStatus", "type": "STRING", "mode": "NULLABLE"},
               {"name": "receiverDeliveryStatusTime", "type": "TIMESTAMP", "mode": "NULLABLE"},
               {"name": "receiverExternalId", "type": "INTEGER", "mode": "NULLABLE"},
               {"name": "receiverEmail", "type": "STRING", "mode": "NULLABLE"},
               {"name": "receiverPhoneNumber", "type": "INTEGER", "mode": "REQUIRED"},
               {"name": "sendTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
               {"name": "sendTimeComplete", "type": "TIMESTAMP", "mode": "REQUIRED"},
               {"name": "senderBaseType", "type": "STRING", "mode": "NULLABLE"},
               {"name": "senderType", "type": "STRING", "mode": "NULLABLE"},
               {"name": "senderName", "type": "STRING", "mode": "NULLABLE"},
               {"name": "senderPhoneNumber", "type": "INTEGER", "mode": "REQUIRED"},
               {"name": "state", "type": "STRING", "mode": "REQUIRED"},
               {"name": "templateName", "type": "STRING", "mode": "NULLABLE"},
               {"name": "tryTimes", "type": "INTEGER", "mode": "NULLABLE"},
               {"name": "CREATE_USER_ID", "type": "STRING", "mode": "NULLABLE"},
               {"name": "CREATE_TS", "type": "TIMESTAMP", "mode": "NULLABLE"},
               {"name": "LAST_UPDT_USER_ID", "type": "STRING", "mode": "NULLABLE"},
               {"name": "LAST_UPDT_TS", "type": "TIMESTAMP", "mode": "NULLABLE"}
               ]
}


class GroupWindowsIntoBatches(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """

    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
                pcoll
                # Assigns window info to each Pub/Sub message based on its
                # publish timestamp.
                | "Window into Fixed Intervals"
                >> beam.WindowInto(window.FixedWindows(self.window_size))
                # | "Add timestamps to messages" >> beam.ParDo(AddTimestamps())
                # Use a dummy key to group the elements in the same window.
                # Note that all the elements in one window must fit into memory
                # for this. If the windowed elements do not fit into memory,
                # please consider using `beam.util.BatchElements`.
                # https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements
                | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
                | "Groupby" >> beam.GroupByKey()
                | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
                | "Get Dictionary" >> beam.Map(lambda x: x[0])
        )


class Printer(beam.DoFn):
    """To Print the output on the Console Using ParDo"""

    def process(self, data_item):
        print(data_item)


class custom_json_flatten(beam.DoFn):

    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        # def process(self, element):
        dict_flattened = flatten(element, "_")
        clean_dict = {key.replace('@', ''): item for key, item in dict_flattened.items()}
        d1 = {'priority': json.dumps(clean_dict.get('priority'))
            , 'logFlag': json.dumps(clean_dict.get('logFlag'))
            , 'receiver_0_billingAccountMastersourceId': json.dumps(
                clean_dict.get('receiver_0_billingAccountMastersourceId'))
            , 'receiver_0_billingAccountNumber': json.dumps(clean_dict.get('receiver_0_billingAccountNumber'))
            , 'tryTimes': json.dumps(clean_dict.get('tryTimes'))}
        clean_dict.update(d1)
        clean_dict['characteristicName'] = clean_dict.pop('characteristic_0_name', None)
        clean_dict['characteristicValue'] = clean_dict.pop('characteristic_0_value', None)
        clean_dict['characteristicValueType'] = clean_dict.pop('characteristic_0_valueType', None)
        clean_dict['receiverBaseType'] = clean_dict.pop('receiver_0_baseType', None)
        clean_dict['receiverType'] = clean_dict.pop('receiver_0_type', None)
        clean_dict['receiverBillingAccountMastersourceId'] = clean_dict.pop('receiver_0_billingAccountMastersourceId',
                                                                            None)
        clean_dict['receiverBillingAccountNumber'] = clean_dict.pop('receiver_0_billingAccountNumber', None)
        clean_dict['receiverDeliveryStatus'] = clean_dict.pop('receiver_0_deliveryStatus', None)
        clean_dict['receiverDeliveryStatusTime'] = clean_dict.pop('receiver_0_deliveryStatusTime', None)
        clean_dict['receiverExternalId'] = clean_dict.pop('receiver_0_externalId', None)
        clean_dict['receiverPhoneNumber'] = clean_dict.pop('receiver_0_phoneNumber', None)
        clean_dict['senderBaseType'] = clean_dict.pop('sender_baseType', None)
        clean_dict['senderType'] = clean_dict.pop('sender_type', None)
        clean_dict['senderName'] = clean_dict.pop('sender_name', None)
        clean_dict['senderPhoneNumber'] = clean_dict.pop('sender_phoneNumber', None)
        clean_dict['df_current_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clean_dict['CREATE_TS'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clean_dict['LAST_UPDT_TS'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clean_dict['CREATE_USER_ID'] = 'pubsub_via_dataflow'
        clean_dict['LAST_UPDT_USER_ID'] = 'pubsub_via_dataflow'
        clean_dict['pubsub_ingestion_timestamp'] = datetime.utcfromtimestamp(float(publish_time)).strftime(
            "%Y-%m-%d %H:%M:%S.%f")
        # clean_dict['pubsub_id'] = Unique_id
        return [clean_dict]


def run(input_topic, output_path, window_size=1.0):
    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )
    argv = [
        '--project={0}'.format(PROJECT_ID),
        '--job_name=pub-sub-to-file',
        '--input_topic=projects/cio-sea-team-lab-9e09db/topics/cron-topic',
        '--output_path=gs://cio-sea-team-lab-9e09db-ecp-project/trial.txt'
        '--region=northamerica-northeast1',
        '--save_main_session',
        '--staging_location=gs://{0}/misc-temp/'.format(BUCKET),
        '--temp_location=gs://{0}/misc-temp/'.format(BUCKET),
        '--streaming=True',
        '--runner=DirectRunner'
    ]

    with beam.Pipeline(options=pipeline_options) as pipeline:
        data_from_source = (
                pipeline
                | "Read PubSub Messages" >> beam.io.ReadFromPubSub(topic=input_topic).with_output_types(bytes)
                # | "Window into" >> GroupWindowsIntoBatches(window_size)
                # | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
                | "PARSE JSON" >> beam.Map(json.loads)
                | "CUSTOM JOSN PARSE" >> beam.ParDo(custom_json_flatten())
            # | "Write to GCS" >> beam.ParDo(WriteBatchesToGCS(output_path))
            # | "PRINT JSON" >> beam.ParDo(Printer())
        )

        load_data = (data_from_source
                     | "JSON WriteDataToBigQuery" >> beam.io.WriteToBigQuery(
                    "{0}:{1}.outbound_contact_event_pubsub".format(PROJECT_ID, datasetId_Staging),
                    schema=table_schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    # write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                     )

    pipeline.run().wait_until_finish()


if __name__ == "__main__":  # noqa
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        default='projects/cio-sea-team-lab-9e09db/topics/cron-topic',
        help="The Cloud Pub/Sub topic to read from.\n"
             '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in number of minutes.",
    )
    parser.add_argument(
        "--output_path",
        default='gs://cio-sea-team-lab-9e09db-ecp-project/trial.txt',
        help="GCS Path of the output file including filename prefix.",
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.input_topic,
        known_args.output_path,
        known_args.window_size)