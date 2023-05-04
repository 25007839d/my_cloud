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
from datetime import datetime

PROJECT_ID = 'cio-sea-team-lab-9e09db'
BUCKET = 'cio-sea-team-lab-9e09db-ecp-project'


# load the Service Account json file to allow GCP resources to be used
# os.environ['GOOGLE_APPLICATION_CREDENTIALS']


class Printer(beam.DoFn):
    """To Print the output on the Console Using ParDo"""

    def process(self, data_item):
        print(data_item)


select_query = 'SELECT \
CAST(\"1234567890\" as INT64) AS CNTCT_EVENT_ID \
, sendTime as CNTCT_EFFECTIVE_START_TS \
, null as CNTCT_EVENT_TYP_ID \
, messageType as CNTCT_MECHANISM_TYP_CD \
, null as CNTCT_CONTENT_DTL_ID \
, sendTimeComplete as CNTCT_EFFECTIVE_END_TS \
, id as CNTCT_EVENT_HEADER_ID \
, sendTime as HEADER_EFFECTIVE_START_TS \
, null as IDENTITY_PROFILE_ID \
, null as CONTACT_ROLE_CD \
, null as PROFILE_UNIVERSALLY_UNIQUE_ID \
, null as CUSTOMER_ID \
, null as CUSTOMER_MASTER_SRC_ID \
, receiverBillingAccountMastersourceId as BILLING_MASTER_SRC_ID \
, receiverBillingAccountNumber as BILLING_ACCOUNT_NUM \
, null as PRODUCT_INSTNC_MASTER_SRC_ID \
, null as PRODUCT_INSTNC_KEY_ID \
, null as RESOURCE_TYP_ID \
, null as RESOURCE_VALUE_ID \
, senderPhoneNumber as SRC_APP_RESOURCE_CD \
, "ecpapplication" as CREATE_USER_ID \
, CURRENT_TIMESTAMP as CREATE_TS \
, "ecpapplication" as LAST_UPDT_USER_ID \
, CURRENT_TIMESTAMP as LAST_UPDT_TS \
, templateName as CNTCT_CONTENT_TEMPLATE_CD \
, category as CNTCT_TEMPLATE_CATGY_CD \
, language as LANGUAGE_CD \
, null as TMPLT_VRSN_ID \
FROM `cio-sea-team-lab-9e09db.ecp_project.outbound_contact_event_pubsub`'

column_header = ['CNTCT_EVENT_ID', \
                 'CNTCT_EFFECTIVE_START_TS', \
                 'CNTCT_EVENT_TYP_ID', \
                 'CNTCT_MECHANISM_TYP_CD', \
                 'CNTCT_CONTENT_DTL_ID', \
                 'CNTCT_EFFECTIVE_END_TS', \
                 'CNTCT_EVENT_HEADER_ID', \
                 'HEADER_EFFECTIVE_START_TS', \
                 'IDENTITY_PROFILE_ID', \
                 'CONTACT_ROLE_CD', \
                 'PROFILE_UNIVERSALLY_UNIQUE_ID', \
                 'CUSTOMER_ID', \
                 'CUSTOMER_MASTER_SRC_ID', \
                 'BILLING_MASTER_SRC_ID', \
                 'BILLING_ACCOUNT_NUM', \
                 'PRODUCT_INSTNC_MASTER_SRC_ID', \
                 'PRODUCT_INSTNC_KEY_ID', \
                 'RESOURCE_TYP_ID', \
                 'RESOURCE_VALUE_ID', \
                 'SRC_APP_RESOURCE_CD', \
                 'CREATE_USER_ID', \
                 'CREATE_TS', \
                 'LAST_UPDT_USER_ID', \
                 'LAST_UPDT_TS', \
                 'CNTCT_CONTENT_TEMPLATE_CD', \
                 'CNTCT_TEMPLATE_CATGY_CD', \
                 'LANGUAGE_CD', \
                 'TMPLT_VRSN_ID']

load_header = 'CNTCT_EVENT_ID|CNTCT_EFFECTIVE_START_TS|CNTCT_EVENT_TYP_ID|CNTCT_MECHANISM_TYP_CD|CNTCT_CONTENT_DTL_ID|CNTCT_EFFECTIVE_END_TS|CNTCT_EVENT_HEADER_ID|HEADER_EFFECTIVE_START_TS|IDENTITY_PROFILE_ID|CONTACT_ROLE_CD|PROFILE_UNIVERSALLY_UNIQUE_ID|CUSTOMER_ID|CUSTOMER_MASTER_SRC_ID|BILLING_MASTER_SRC_ID|BILLING_ACCOUNT_NUM|PRODUCT_INSTNC_MASTER_SRC_ID|PRODUCT_INSTNC_KEY_ID|RESOURCE_TYP_ID|RESOURCE_VALUE_ID|SRC_APP_RESOURCE_CD|CREATE_USER_ID|CREATE_TS|LAST_UPDT_USER_ID|LAST_UPDT_TS|CNTCT_CONTENT_TEMPLATE_CD|CNTCT_TEMPLATE_CATGY_CD|LANGUAGE_CD|TMPLT_VRSN_ID'

from csv import DictWriter
from csv import excel
from io import StringIO


def _dict_to_csv(element, column_order, missing_val='', discard_extras=True, dialect=excel, delimiter='|'):
    """ Additional properties for delimiters, escape chars, etc via an instance of csv.Dialect
        Note: This implementation does not support unicode
    """
    from csv import DictWriter
    from csv import excel
    from io import StringIO

    buf = StringIO()

    writer = DictWriter(buf,
                        fieldnames=column_order,
                        restval=missing_val,
                        extrasaction=('ignore' if discard_extras else 'raise'),
                        dialect=dialect,
                        delimiter='|')
    writer.writerow(element)

    return buf.getvalue().rstrip(dialect.lineterminator)


class _DictToCSVFn(beam.DoFn):
    """ Converts a Dictionary to a CSV-formatted String

        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in the input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect

    """
    from csv import DictWriter
    from csv import excel
    from io import StringIO
    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel, delimiter='|'):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect
        self._delimiter = delimiter

    def process(self, element, *args, **kwargs):
        result = _dict_to_csv(element,
                              column_order=self._column_order,
                              missing_val=self._missing_val,
                              discard_extras=self._discard_extras,
                              dialect=self._dialect,
                              delimiter=self._delimiter)

        return [result, ]


class DictToCSV(beam.PTransform):
    """ Transforms a PCollection of Dictionaries to a PCollection of CSV-formatted Strings

        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in an input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect

    """
    from csv import DictWriter
    from csv import excel
    from io import StringIO
    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel, delimiter='|'):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect
        self._delimiter = delimiter

    def expand(self, pcoll):
        return pcoll | beam.ParDo(_DictToCSVFn(column_order=self._column_order,
                                               missing_val=self._missing_val,
                                               discard_extras=self._discard_extras,
                                               dialect=self._dialect,
                                               delimiter=self._delimiter)
                                  )


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_path',
        dest='output_path',
        default=None,
        help='Output file to write the target bucket/path')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(WorkerOptions).use_public_ips = False

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the table r(options = pipeline_optionsows into a PCollection.
        rows = p | 'READ FROM Staging Table' >> beam.io.Read(
            beam.io.BigQuerySource(query=select_query, use_standard_sql=True))

        # Write the output using a "Write" transform that has side effects.
        x = (rows
             | 'Parse to CSV' >> DictToCSV(column_order=column_header)
             # |'Print' >>  beam.ParDo(Printer())
             | 'WriteToText' >> beam.io.WriteToText(known_args.output_path
                                                    # 'gs://cio-sea-team-lab-9e09db-ecp-project/tgtoutput/CNTCT_EVENT'
                                                    , file_name_suffix='.csv'
                                                    , header=load_header)
             )