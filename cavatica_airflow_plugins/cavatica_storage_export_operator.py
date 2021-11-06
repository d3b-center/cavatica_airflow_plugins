# -*- coding: utf-8 -*-
from base64 import b64decode
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from cavatica_airflow_plugins.cavatica_sensor import CavaticaTaskSensor

logging.basicConfig(format='%(asctime)s - %(levelname)s:%(message)s', level=logging.DEBUG)


class CavaticaStorageExportOperator(BaseOperator):
    """Uses the Cavatica API to export a file to S3 storage.

    https://docs.cavatica.org/docs/start-an-export-job-v2
    https://docs.cavatica.org/docs/get-details-of-an-export-job-v2

    The CavaticaTaskSensor is imported here to monitor the export job. This Operator 
    requires HTTP headers and passes them down to the CavaticaTaskSensor. This is
    to ensure the sensor has permissions to monitor the job in case two different
    Airflow connections are used.

    This Operator will return True when the export job has completed successfully,
    or fail the DAG if the export job fails.

    cavatica_conn_id: name of the Airflow Connection that points to Cavatica.
        type:       str
        example:    cavatica
    cavatica_headers: HTTP request headers for the Cavatica API
        type:       dict
        example:    {"Content-Type": "application/json", "X-SBG-Auth-Token": <token>}
    source_file_uri: Cavatica "path ID" that identifies the unique file to be exported
        type:       str
        example:    610aa71f2f5730089b081ea4
    destination_volume: name of the s3 as mounted to Cavatica (i.e., not the s3 path itself)
        type:       str
        example:    volume/folder    
    destination_location: name of the s3 key to be created for the file relative to destination_volume
        type:       str
        example:    /bams/example.bam (would create volume/folder/bams/example.bam)
    optional_fields: optional, other key-value pairs the Cavatica endpoint accepts
        type:       dict
        example:    {"overwrite": True, "sse_algorithm": "AES256"}


    returns True
    """

    ui_color = '#e811fc'
    endpoint = '/storage/exports'

    @apply_defaults
    def __init__(self,
                 cavatica_conn_id,
                 cavatica_headers,
                 source_file_uri,
                 destination_volume,
                 destination_location,
                 optional_fields={},
                 *args,
                 **kwargs
                 ):
        super(CavaticaStorageExportOperator, self).__init__(*args, **kwargs)
        self.cavatica_conn_id = cavatica_conn_id
        self.cavatica_headers = cavatica_headers
        self.source_file_uri = source_file_uri
        self.destination_volume = destination_volume
        self.destination_location = destination_location
        self.optional_fields = optional_fields

    def execute(self, context):
        """Start export job and wait for COMPLETED from CavaticaTaskSensor."""

        payload = {
            "source": {
                "file": self.source_file_uri
            },
            "destination": {
                "volume": self.destination_volume,
                "location": self.destination_location
            }
        }

        if self.optional_fields:
            for key in self.optional_fields.keys():
                payload[key] = self.optional_fields[key]

        api = HttpHook(method='POST', http_conn_id=self.cavatica_conn_id)
        response = api.run(endpoint=endpoint, json=payload, headers=self.cavatica_headers)
        response.raise_for_status()

        export_task_id = response.json()["id"]

        wait_for_export_success = CavaticaTaskSensor(
            task_id='wait_for_export_success',
            cavatica_task_id=export_task_id,
            cavatica_conn_id=self.cavatica_conn_id,
            cavatica_headers=self.cavatica_headers,
            endpoint='{endpoint}/',
            poke=10,
            timeout=3600
        )
        wait_for_export_success.execute(context)
