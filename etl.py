import json
import os
from datetime import datetime
from typing import Any, Dict

import apache_beam as beam
import pandas as pd
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.mongodbio import ReadFromMongoDB
from apache_beam.options.pipeline_options import (PipelineOptions,
                                                  SetupOptions,
                                                  StandardOptions)
from bson import ObjectId, json_util
from dotenv import load_dotenv

load_dotenv()

class MongoDBToBigQueryOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        """ """
        parser.add_argument('--project_id', required=False, help='GCP project ID', default=os.getenv('PROJECT'))
        parser.add_argument('--dataset', required=False, help='BigQuery dataset', default=os.getenv('DATASET'))
        parser.add_argument('--file_name', required=False, help='Collections to move', default=os.getenv('FILE_NAME'))
        parser.add_argument('--mongo_username', required=False, help='MongoDB Username', default=os.getenv('MONGODB_USERNAME'))
        parser.add_argument('--mongo_password', required=False, help='MongoDB Password', default=os.getenv('MONGODB_PASSWORD'))

def process_document(document: Dict[str, Any]) -> Dict[str, Any]:
    """Transform the mongo record into bigquery raw schema table format"""
    mongo_id = str(document.get('_id', ''))
    if isinstance(document.get('_id'), ObjectId):
        mongo_id = str(document['_id'])
    full_document = json.loads(json_util.dumps(document))
    
    return {
        'id': mongo_id,
        'raw': json.dumps(full_document),
        'clusterTime': datetime.utcnow().isoformat(),
        'op': 1
    }

def get_file(filename: str) -> pd.DataFrame:
    """read csv file"""
    try:
        df = pd.read_csv(filename)
        return df
    except Exception as e:
        print(e)
    return pd.DataFrame()

def create_pipeline(row: pd.Series, pipeline_options: PipelineOptions) -> beam.Pipeline:
    """Create pipeline for reading data from a MongoDB Collection and writing the results to BigQuery"""

    table_name = row['table_name'].replace('.','_')
    collection_name = row['collection_name']
    database_name = row['database_name']
    host_name = f"mongodb://{pipeline_options.mongo_username}:{pipeline_options.mongo_password}@{row['host_name']}"

    table_schema = {
        "fields": [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "raw", "type": "STRING", "mode": "NULLABLE"},
            {"name": "clusterTime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "op", "type": "INTEGER", "mode": "NULLABLE"}
        ]
    }

    p = beam.Pipeline(options=pipeline_options)

    (p 
    | 'Read from MongoDB' >> ReadFromMongoDB(
        uri=host_name,
        db=database_name,
        coll=collection_name
    )
    | 'Process Documents' >> beam.Map(process_document)
    | 'Write to BigQuery' >>  WriteToBigQuery(
        table=f"{pipeline_options.project_id}:{pipeline_options.dataset}.{table_name}",
        schema=parse_table_schema_from_json(json.dumps(table_schema)),
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method='STREAMING_INSERTS'
    ))

    return p

def run_pipeline(pipeline: beam.Pipeline) -> None:
    """ """
    try:
        pipeline.run()
    except Exception as e:
        print(f"Pipeline execution failed: {e}")

def main() -> None:
    """ """
    pipeline_options = MongoDBToBigQueryOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'

    data = get_file(pipeline_options.file_name)
    for index, row in data.iterrows():
        print(index)
        pipeline = create_pipeline(row, pipeline_options)
        run_pipeline(pipeline)

if __name__ == '__main__':
    main()