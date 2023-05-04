#!/usr/bin/env python
# coding: utf-8

# In[1]:


from __future__ import absolute_import
from __future__ import division

import argparse
import logging
import sys
import os
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.avroio import WriteToAvro
import fastavro

from datetime import datetime
import json

import threading
import time
import types
from apache_beam.runners.runner import PipelineState

# In[2]:


PROJECT_ID = 'cio-sea-team-lab-9e09db'
BUCKET = 'cio-sea-team-lab-9e09db-ecp-project'
# load the Service Account json file to allow GCP resources to be used
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "cio-sea-team-lab-9e09db-0b2782fa290b.json"


class Printer(beam.DoFn):
    """To Print the output on the Console Using ParDo"""

    def process(self, data_item):
        print(data_item)


# In[3]:


select_query = 'SELECT CAST("1234567890" as INT64) AS CNTCT_EVENT_ID , FORMAT_TIMESTAMP(\'%F %T\',sendTime) as CNTCT_EFFECTIVE_START_TS , null as CNTCT_EVENT_TYP_ID , messageType as CNTCT_MECHANISM_TYP_CD , null as CNTCT_CONTENT_DTL_ID , FORMAT_TIMESTAMP(\'%F %T\',sendTimeComplete) as CNTCT_EFFECTIVE_END_TS , id as CNTCT_EVENT_HEADER_ID , FORMAT_TIMESTAMP(\'%F %T\',sendTime) as HEADER_EFFECTIVE_START_TS , null as IDENTITY_PROFILE_ID , null as CONTACT_ROLE_CD , null as PROFILE_UNIVERSALLY_UNIQUE_ID , null as CUSTOMER_ID , null as CUSTOMER_MASTER_SRC_ID , receiverBillingAccountMastersourceId as BILLING_MASTER_SRC_ID , receiverBillingAccountNumber as BILLING_ACCOUNT_NUM , null as PRODUCT_INSTNC_MASTER_SRC_ID , null as PRODUCT_INSTNC_KEY_ID , null as RESOURCE_TYP_ID , null as RESOURCE_VALUE_ID , senderPhoneNumber as SRC_APP_RESOURCE_CD , "ecpapplication" as CREATE_USER_ID , FORMAT_TIMESTAMP(\'%F %T\', CURRENT_TIMESTAMP) as CREATE_TS , "ecpapplication" as LAST_UPDT_USER_ID , FORMAT_TIMESTAMP(\'%F %T\', CURRENT_TIMESTAMP) as LAST_UPDT_TS , templateName as CNTCT_CONTENT_TEMPLATE_CD , category as CNTCT_TEMPLATE_CATGY_CD , language as LANGUAGE_CD , null as TMPLT_VRSN_ID FROM `cio-sea-team-lab-9e09db.ecp_project.outbound_contact_event_pubsub`'

SCHEMA = {"namespace": "example.avro"
    , "name": "Transaction"
    , "type": "record"
    , "fields": [
        {"name": "CNTCT_EVENT_ID", "type": {"logicalType": "decimal", "type": "int"}},
        {"name": "CNTCT_EFFECTIVE_START_TS", "type": {"type": "string", "logicalType": "timestamp-millis"}},
        {"name": "CNTCT_EVENT_TYP_ID", "type": ["null", "string"]},
        {"name": "CNTCT_MECHANISM_TYP_CD", "type": ["null", "string"]},
        {"name": "CNTCT_CONTENT_DTL_ID", "type": ["null", "int"]},
        {"name": "CNTCT_EFFECTIVE_END_TS", "type": {"type": "string", "logicalType": "timestamp-millis"}},
        {"name": "CNTCT_EVENT_HEADER_ID", "type": ["null", "string"]},
        {"name": "HEADER_EFFECTIVE_START_TS", "type": {"type": "string", "logicalType": "timestamp-millis"}},
        {"name": "IDENTITY_PROFILE_ID", "type": ["null", "int"]},
        {"name": "CONTACT_ROLE_CD", "type": ["null", "string"]},
        {"name": "PROFILE_UNIVERSALLY_UNIQUE_ID", "type": ["null", "int"]},
        {"name": "CUSTOMER_ID", "type": ["null", "int"]},
        {"name": "CUSTOMER_MASTER_SRC_ID", "type": ["null", "int"]},
        {"name": "BILLING_MASTER_SRC_ID", "type": ["null", "int"]},
        {"name": "BILLING_ACCOUNT_NUM", "type": ["null", "int"]},
        {"name": "PRODUCT_INSTNC_MASTER_SRC_ID", "type": ["null", "int"]},
        {"name": "PRODUCT_INSTNC_KEY_ID", "type": ["null", "int"]},
        {"name": "RESOURCE_TYP_ID", "type": ["null", "int"]},
        {"name": "RESOURCE_VALUE_ID", "type": ["null", "int"]},
        {"name": "SRC_APP_RESOURCE_CD", "type": ["null", "int"]},
        {"name": "CREATE_USER_ID", "type": ["null", "string"]},
        {"name": "CREATE_TS", "type": {"type": "string", "logicalType": "timestamp-millis"}},
        {"name": "LAST_UPDT_USER_ID", "type": ["null", "string"]},
        {"name": "LAST_UPDT_TS", "type": {"type": "string", "logicalType": "timestamp-millis"}},
        {"name": "CNTCT_CONTENT_TEMPLATE_CD", "type": ["null", "string"]},
        {"name": "LANGUAGE_CD", "type": ["null", "string"]},
        {"name": "TMPLT_VRSN_ID", "type": ["null", "int"]}
    ]
          }

schema_parsed = fastavro.parse_schema(SCHEMA)


# In[5]:


def run(argv=None):
    pipeline_args = [
        '--project={0}'.format(PROJECT_ID),
        '--job_name=bq-to-bq-dtl',
        '--region=northamerica-northeast1',
        '--save_main_session',
        '--staging_location=gs://{0}/misc-temp/'.format(BUCKET),
        '--temp_location=gs://{0}/misc-temp/'.format(BUCKET),
        '--subnetwork=https://www.googleapis.com/compute/alpha/projects/cio-sea-team-lab-9e09db/regions/northamerica-northeast1/subnetworks/sealabsubnet',
        '--runner=DirectRunner'
    ]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_path',
        dest='output_path',
        default=None,
        help='Output file to write the target bucket/path')

    # change the pipeline_args to argv when running from Command line
    known_args, pipeline_args = parser.parse_known_args(pipeline_args)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(WorkerOptions).use_public_ips = False

    # with beam.Pipeline(options=pipeline_options) as p:
    p = beam.Pipeline(options=pipeline_options)
    # Read the table r(options = pipeline_optionsows into a PCollection.
    rows = p | 'READ FROM Staging Table' >> beam.io.ReadFromBigQuery(query=select_query
                                                                     , use_standard_sql=True
                                                                     )

    # Write the output using a "Write" transform that has side effects.
    x = (rows
         # |'Print' >>  beam.ParDo(Printer())
         | 'Write to Avro' >> WriteToAvro(
                "gs://cio-sea-team-lab-9e09db-ecp-project/tgtoutput/CNTCT_EVENT/cntc_event_" + datetime.now().strftime(
                    "%Y%m%d%H%M")
                , file_name_suffix='.avro'
                , schema=schema_parsed
                )
         )

    p.run().wait_until_finish()


if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.INFO)
    run()

