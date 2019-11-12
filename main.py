# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


'''
This Cloud function is responsible for:
- Parsing and validating new files added to Cloud Storage.
- Checking for duplications.
- Inserting files' content into BigQuery.
- Logging the ingestion status into Cloud Firestore and Stackdriver.
- Publishing a message to either an error or success topic in Cloud Pub/Sub.
'''

import json
import logging
import os
import traceback
from datetime import datetime

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import storage
import pytz



PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'ep'
BQ_TABLE = 'ep_fn_feeded'

CS = storage.Client()
BQ = bigquery.Client()


def streaming(data, context):
    '''This function is executed whenever a file is added to Cloud Storage'''
    bucket_name = data['bucket']
    file_name = data['name']
    logging.info('Loading \'%s\' file from \'%s\' bucket to \'%s\':\'%s\'.\'%s\' BQ table ...' % (file_name, bucket_name, PROJECT_ID, BQ_DATASET, BQ_TABLE) )

    try:
        _insert_into_bigquery(bucket_name, file_name)
        _handle_success(db_ref)
    except Exception:
        _handle_error(db_ref)



def _run_bq_load_job(bucket_name, file_name):
    dataset_ref = BQ.dataset(BQ_DATASET)

    job_config = bigquery.LoadJobConfig()
    #job_config.schema = [
    #    bigquery.SchemaField("name", "STRING"),
    #    bigquery.SchemaField("post_abbr", "STRING"),
    #]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    uri = "gs://{}/{}".format(bucket_name, file_name)
    logging.info('GCS URI \'%s\'' % (uri) )
    
    load_job = client.load_table_from_uri(
            uri, 
            dataset_ref.table(BQ_TABLE), 
            job_config=job_config
        )
    logging.info('BQ Load Job started ...')

    load_job.result()
    logging.info('BQ Load Job completed ...')


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')

