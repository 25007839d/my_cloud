from __future__ import absolute_import

import argparse
import logging
import re
import os
import sys

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

import json
import pandas as pd
from datetime import datetime

from flatten_json import flatten
import xmltodict

PROJECT_ID = 'cio-sea-team-lab-9e09db'
BUCKET = 'cio-sea-team-lab-9e09db-ecp-project'
JOB_NAME = 'file-json-with-xml-to-file'
datasetId_Staging = 'ecp_project'

xml_table_schema = {
    'fields': [{"name": "pubsub_id", "type": "INTEGER", "mode": "NULLABLE"},
               {"name": "pubsub_ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
               {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
               {'name': 'xmlns', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'xmlnsxsi', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'access_login', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'AccountNumber', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'AccountType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'AccountSubType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'NotificationCode', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'NotificationZone', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'NotificationZone_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'RatingZone_lang_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'RatingZone_lang_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'StartCycleDate', 'type': 'DATE', 'mode': 'NULLABLE'},
               {'name': 'EndCycleDate', 'type': 'DATE', 'mode': 'NULLABLE'},
               {'name': 'LocalEventTime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
               {'name': 'NotificationTimeZone', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'ExternalOfferCode', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationTotalVolumeMB', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'PPUChargedData', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationAverageVolumeMB', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationEstimatedRemainingVolumeMB', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationBillCycleElapsedDays', 'type': 'INTEGER', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationBillCycleRemainingDays', 'type': 'INTEGER', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationUnit', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationThresholdCrossed', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationAllowanceSizeMB', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationAllowanceLeftMB', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationAllowanceExpiration', 'type': 'DATE', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationAllowanceExpirationTime', 'type': 'TIME', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationPriceType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationPriceExpirationDate', 'type': 'DATE', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationPriceExpirationTime', 'type': 'TIME', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationFlatRate', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationFlatUnit_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationFlatUnit_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationTierType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationTierSizeValue', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationTierSizeUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationTierSizeUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationTierRateValue', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationTierRateUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationTierRateUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationDurationPrice', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationDurationPeriod', 'type': 'INTEGER', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationDurationPeriodUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationDurationPeriodUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationDurationQty', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationDurationUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'UsageNotificationDurationUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockNotificationType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationThresholdCrossed', 'type': 'INTEGER', 'mode': 'NULLABLE'},
               {'name': 'BlockNotificationUnit', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationSelfUnblockFlag', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationAllowanceSizeMB', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationAllowanceLeftMB', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextPriceType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationAllowanceExpiration', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextPriceExpiration', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationAllowanceSize', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationAllowanceUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationAllowanceUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationOverageRateValue', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationOverageRateUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationOverageRateUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextTierType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextTierSizeValue', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextTierSizeUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextTierSizeUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextTierRateValue', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextTierRateUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextTierRateUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextFlatRateValue', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextFlatRateUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextFlatRateUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextDurationPrice', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextDurationPeriod', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextDurationPeriodUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextDurationPeriodUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextDurationQty', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextDurationUOM_en', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'BlockingNotificationNextDurationUOM_fr', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'accountDetail_accountSegment', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'accountDetail_accountSubSegment', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'accountDetail_billCycle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
               {'name': 'accountDetail_brandId', 'type': 'INTEGER', 'mode': 'NULLABLE'},
               {'name': 'accountDetail_collectionStatus', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'accountDetail_creditClass', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'accountDetail_creditLimit', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'subscriberDetail_IMSI', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'subscriberDetail_language', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'subscriberDetail_networkType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'subscriberDetail_province', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'subscriberDetail_ratePlanCode', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'usageDetail_networkTypeRAT', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'usageDetail_deviceType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'usageDetail_MCCMNC', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'usageDetail_countryCode', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'usageDetail_planRecurrenceType', 'type': 'STRING', 'mode': 'NULLABLE'},
               {'name': 'usageDetail_planPriceType', 'type': 'STRING', 'mode': 'NULLABLE'}
               ]
}


class Printer(beam.DoFn):
    """To Print the output on the Console Using ParDo"""

    def process(self, data_item):
        print(data_item)


class custom_XML_flatten(beam.DoFn):

    def process(self, element):
        xml_content = xmltodict.parse(element['characteristicValue'])['ProductUsageNotificationWmktg'][
            'ProductUsageNotification']
        # xml_content = xmltodict.parse(element)['ProductUsageNotificationWmktg']
        output_dict = json.loads(json.dumps(xml_content))
        if output_dict['RatingZone'][0]['@lang'] == 'en' or output_dict['RatingZone'][1]['@lang'] == 'fr':
            d2 = {'RatingZone_lang_en': output_dict['RatingZone'][0]['#text'],
                  'RatingZone_lang_fr': output_dict['RatingZone'][1]['#text']}
            output_dict.update(d2)
            del output_dict['RatingZone']
        UsageNotificationFlatUnit_en
        UsageNotificationTierSizeUOM_en
        UsageNotificationTierRateUOM_en
        UsageNotificationDurationPeriodUOM_en
        dict_flattened = flatten(output_dict, "_")
        clean_dict = {key.replace('@', ''): item for key, item in dict_flattened.items()}
        clean_dict2 = {key.replace(':', ''): item for key, item in clean_dict.items()}

        clean_dict2['UsageNotificationUnit'] = clean_dict2.pop('usageNotification_UsageNotificationUnit', None)
        clean_dict2['UsageNotificationThresholdCrossed'] = clean_dict2.pop(
            'usageNotification_UsageNotificationThresholdCrossed', None)
        clean_dict2['UsageNotificationAllowanceSizeMB'] = clean_dict2.pop(
            'usageNotification_UsageNotificationAllowanceSizeMB', None)
        clean_dict2['UsageNotificationAllowanceLeftMB'] = clean_dict2.pop(
            'usageNotification_UsageNotificationAllowanceLeftMB', None)
        clean_dict2['UsageNotificationAllowanceExpiration'] = clean_dict2.pop(
            'usageNotification_UsageNotificationAllowanceExpiration', None)
        clean_dict2['UsageNotificationAllowanceExpirationTime'] = clean_dict2.pop(
            'usageNotification_UsageNotificationAllowanceExpirationTime', None)
        clean_dict2['UsageNotificationPriceType'] = clean_dict2.pop('usageNotification_UsageNotificationPriceType',
                                                                    None)
        clean_dict2['UsageNotificationPriceExpirationDate'] = clean_dict2.pop(
            'usageNotification_UsageNotificationPriceExpirationDate', None)
        clean_dict2['UsageNotificationPriceExpirationTime'] = clean_dict2.pop(
            'usageNotification_UsageNotificationPriceExpirationTime', None)
        clean_dict2['UsageNotificationFlatRate'] = clean_dict2.pop('usageNotification_UsageNotificationFlatRate', None)
        clean_dict2['UsageNotificationFlatUnit_en'] = clean_dict2.pop('usageNotification_UsageNotificationFlatUnit_en',
                                                                      None)
        clean_dict2['UsageNotificationFlatUnit_fr'] = clean_dict2.pop('usageNotification_UsageNotificationFlatUnit_fr',
                                                                      None)

        clean_dict2['UsageNotificationTierType'] = clean_dict2.pop(
            'usageNotification_tierTypeUsageNotification_UsageNotificationTierType', None)
        clean_dict2['UsageNotificationTierSizeValue'] = clean_dict2.pop(
            'usageNotification_tierTypeUsageNotification_UsageNotificationTierSizeValue', None)
        clean_dict2['UsageNotificationTierSizeUOM_en'] = clean_dict2.pop(
            'usageNotification_tierTypeUsageNotification_UsageNotificationTierSizeUOM_en', None)
        clean_dict2['UsageNotificationTierSizeUOM_fr'] = clean_dict2.pop(
            'usageNotification_tierTypeUsageNotification_UsageNotificationTierSizeUOM_fr', None)
        clean_dict2['UsageNotificationTierRateValue'] = clean_dict2.pop(
            'usageNotification_tierTypeUsageNotification_UsageNotificationTierRateValue', None)
        clean_dict2['UsageNotificationTierRateUOM_en'] = clean_dict2.pop(
            'usageNotification_tierTypeUsageNotification_UsageNotificationTierRateUOM_en', None)
        clean_dict2['UsageNotificationTierRateUOM_fr'] = clean_dict2.pop(
            'usageNotification_tierTypeUsageNotification_UsageNotificationTierRateUOM_fr', None)
        clean_dict2['UsageNotificationDurationPrice'] = clean_dict2.pop(
            'usageNotification_durationTypeUsageNotification_UsageNotificationDurationPrice', None)
        clean_dict2['UsageNotificationDurationPeriod'] = clean_dict2.pop(
            'usageNotification_durationTypeUsageNotification_UsageNotificationDurationPeriod', None)
        clean_dict2['UsageNotificationDurationPeriodUOM_en'] = clean_dict2.pop(
            'usageNotification_durationTypeUsageNotification_UsageNotificationDurationPeriodUOM_en', None)
        clean_dict2['UsageNotificationDurationPeriodUOM_fr'] = clean_dict2.pop(
            'usageNotification_durationTypeUsageNotification_UsageNotificationDurationPeriodUOM_fr', None)
        clean_dict2['UsageNotificationDurationQty'] = clean_dict2.pop(
            'usageNotification_durationTypeUsageNotification_UsageNotificationDurationQty', None)
        clean_dict2['UsageNotificationDurationUOM_en'] = clean_dict2.pop(
            'usageNotification_durationTypeUsageNotification_UsageNotificationDurationUOM_en', None)
        clean_dict2['UsageNotificationDurationUOM_fr'] = clean_dict2.pop(
            'usageNotification_durationTypeUsageNotification_UsageNotificationDurationUOM_fr', None)

        clean_dict2['BlockNotificationType'] = clean_dict2.pop('blockNotification_BlockNotificationType', None)
        clean_dict2['BlockingNotificationThresholdCrossed'] = clean_dict2.pop(
            'blockNotification_BlockingNotificationThresholdCrossed', None)
        clean_dict2['BlockNotificationUnit'] = clean_dict2.pop('blockNotification_BlockNotificationUnit', None)
        clean_dict2['BlockingNotificationSelfUnblockFlag'] = clean_dict2.pop(
            'blockNotification_BlockingNotificationSelfUnblockFlag', None)
        clean_dict2['BlockingNotificationAllowanceSizeMB'] = clean_dict2.pop(
            'blockNotification_BlockingNotificationAllowanceSizeMB', None)
        clean_dict2['BlockingNotificationAllowanceLeftMB'] = clean_dict2.pop(
            'blockNotification_BlockingNotificationAllowanceLeftMB', None)
        clean_dict2['BlockingNotificationNextPriceType'] = clean_dict2.pop(
            'blockNotification_BlockingNotificationNextPriceType', None)
        clean_dict2['BlockingNotificationAllowanceExpiration'] = clean_dict2.pop(
            'blockNotification_BlockingNotificationAllowanceExpiration', None)
        clean_dict2['BlockingNotificationNextPriceExpiration'] = clean_dict2.pop(
            'blockNotification_BlockingNotificationNextPriceExpiration', None)

        clean_dict2['BlockingNotificationAllowanceSize'] = clean_dict2.pop(
            'blockNotification_allowanceTypeBlockingNotification_BlockingNotificationAllowanceSize', None)
        clean_dict2['BlockingNotificationAllowanceUOM_en'] = clean_dict2.pop(
            'blockNotification_allowanceTypeBlockingNotification_BlockingNotificationAllowanceUOM_en', None)
        clean_dict2['BlockingNotificationAllowanceUOM_fr'] = clean_dict2.pop(
            'blockNotification_allowanceTypeBlockingNotification_BlockingNotificationAllowanceUOM_fr', None)
        clean_dict2['BlockingNotificationOverageRateValue'] = clean_dict2.pop(
            'blockNotification_allowanceTypeBlockingNotification_BlockingNotificationOverageRateValue', None)
        clean_dict2['BlockingNotificationOverageRateUOM_en'] = clean_dict2.pop(
            'blockNotification_allowanceTypeBlockingNotification_BlockingNotificationOverageRateUOM_en', None)
        clean_dict2['BlockingNotificationOverageRateUOM_fr'] = clean_dict2.pop(
            'blockNotification_allowanceTypeBlockingNotification_BlockingNotificationOverageRateUOM_fr', None)

        clean_dict2['BlockingNotificationNextTierType'] = clean_dict2.pop(
            'blockNotification_tierTypeBlockingNotification_BlockingNotificationNextTierType', None)
        clean_dict2['BlockingNotificationNextTierSizeValue'] = clean_dict2.pop(
            'blockNotification_tierTypeBlockingNotification_BlockingNotificationNextTierSizeValue', None)
        clean_dict2['BlockingNotificationNextTierSizeUOM_en'] = clean_dict2.pop(
            'blockNotification_tierTypeBlockingNotification_BlockingNotificationNextTierSizeUOM_en', None)
        clean_dict2['BlockingNotificationNextTierSizeUOM_fr'] = clean_dict2.pop(
            'blockNotification_tierTypeBlockingNotification_BlockingNotificationNextTierSizeUOM_fr', None)
        clean_dict2['BlockingNotificationNextTierRateValue'] = clean_dict2.pop(
            'blockNotification_tierTypeBlockingNotification_BlockingNotificationNextTierRateValue', None)
        clean_dict2['BlockingNotificationNextTierRateUOM_en'] = clean_dict2.pop(
            'blockNotification_tierTypeBlockingNotification_BlockingNotificationNextTierRateUOM_en', None)
        clean_dict2['BlockingNotificationNextTierRateUOM_fr'] = clean_dict2.pop(
            'blockNotification_tierTypeBlockingNotification_BlockingNotificationNextTierRateUOM_fr', None)
        clean_dict2['BlockingNotificationNextFlatRateValue'] = clean_dict2.pop(
            'blockNotification_flatTypeBlockingNotification_BlockingNotificationNextFlatRateValue', None)
        clean_dict2['BlockingNotificationNextFlatRateUOM_en'] = clean_dict2.pop(
            'blockNotification_flatTypeBlockingNotification_BlockingNotificationNextFlatRateUOM_en', None)
        clean_dict2['BlockingNotificationNextFlatRateUOM_fr'] = clean_dict2.pop(
            'blockNotification_flatTypeBlockingNotification_BlockingNotificationNextFlatRateUOM_fr', None)

        clean_dict2['BlockingNotificationNextDurationPrice'] = clean_dict2.pop(
            'blockNotification_durationTypeBlockingNotification_BlockingNotificationNextDurationPrice', None)
        clean_dict2['BlockingNotificationNextDurationPeriod'] = clean_dict2.pop(
            'blockNotification_durationTypeBlockingNotification_BlockingNotificationNextDurationPeriod', None)
        clean_dict2['BlockingNotificationNextDurationPeriodUOM_en'] = clean_dict2.pop(
            'blockNotification_durationTypeBlockingNotification_BlockingNotificationNextDurationPeriodUOM_en', None)
        clean_dict2['BlockingNotificationNextDurationPeriodUOM_fr'] = clean_dict2.pop(
            'blockNotification_durationTypeBlockingNotification_BlockingNotificationNextDurationPeriodUOM_fr', None)
        clean_dict2['BlockingNotificationNextDurationQty'] = clean_dict2.pop(
            'blockNotification_durationTypeBlockingNotification_BlockingNotificationNextDurationQty', None)
        clean_dict2['BlockingNotificationNextDurationUOM_en'] = clean_dict2.pop(
            'blockNotification_durationTypeBlockingNotification_BlockingNotificationNextDurationUOM_en', None)
        clean_dict2['BlockingNotificationNextDurationUOM_fr'] = clean_dict2.pop(
            'blockNotification_durationTypeBlockingNotification_BlockingNotificationNextDurationUOM_fr', None)

        clean_dict2['id'] = element['id']
        clean_dict2['pubsub_ingestion_timestamp'] = element['pubsub_ingestion_timestamp']

        """
        #THIS CODE WILL BE REQUIRED IN FUTURE IF MARKETING XML TAGS IS REQUIRED#
        output_dict = json.loads(json.dumps(xml_content))
        if output_dict['ProductUsageNotification']['RatingZone'][0]['@lang'] == 'en' or output_dict['ProductUsageNotification']['RatingZone'][1]['@lang'] == 'fr':
            d2 = {'RatingZone_lang_en': output_dict['ProductUsageNotification']['RatingZone'][0]['#text'], 'RatingZone_lang_fr': output_dict['ProductUsageNotification']['RatingZone'][1]['#text']}
            output_dict.update(d2)
            del output_dict['ProductUsageNotification']['RatingZone']

        clean_dict = {key.replace('@', ''): item for key, item in dict_flattened.items()}
        clean_dict1 = {key.replace('ProductUsageNotification_', ''): item for key, item in clean_dict.items()}
        clean_dict2 = {key.replace(':', ''): item for key, item in clean_dict1.items()}

        clean_dict2['marketingPreferenceOptInInd'] = clean_dict2.pop('marketingNotification_marketingPreferenceOptInInd', None)
        clean_dict2['notificationLanguageCd'] = clean_dict2.pop('marketingNotification_notificationLanguageCd', None)
        clean_dict2['notificationRoleCd'] = clean_dict2.pop('marketingNotification_notificationRoleCd, None)
        """
        return [clean_dict2]


def run():
    """The main function which creates the pipeline and runs it."""

    argv = [
        '--project={0}'.format(PROJECT_ID),
        '--job_name=bqXML-to-bq',
        '--region=northamerica-northeast1',
        '--save_main_session',
        '--staging_location=gs://{0}/misc-temp/'.format(BUCKET),
        '--temp_location=gs://{0}/misc-temp/'.format(BUCKET),
        '--runner=DirectRunner'
    ]

    with beam.Pipeline(argv=argv) as p:
        data_for_xml = (p
                        | "Read from BigQuery" >> beam.io.Read(beam.io.BigQuerySource(
                    query='Select pubsub_id, pubsub_ingestion_timestamp, characteristicValue, id from `cio-sea-team-lab-9e09db.ecp_project.outbound_contact_event_pubsub` where category = "USAGE"',
                    use_standard_sql=True))
                        # | "FILTER CATEGORY_USAGE DATA" >> beam.Filter(lambda row: row['category'] == 'USAGE')
                        # | "SELECT XML COLUMN" >> beam.Map(lambda record: record['characteristicValue'])
                        | "PARSE XML" >> beam.ParDo(custom_XML_flatten())
                        # | "PRINT XML" >>  beam.ParDo(Printer())
                        )
        load_xml_data_to_bq = (data_for_xml
                               | "XML WriteDataToBigQuery" >> beam.io.WriteToBigQuery(
                    "{0}:{1}.usage_event_content".format(PROJECT_ID, datasetId_Staging),
                    schema=xml_table_schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    # write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                               )

        p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
