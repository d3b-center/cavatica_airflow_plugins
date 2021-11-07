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


class CavaticaStorageImportOperator(BaseOperator):
    """Uses the Cavatica API to import an S3 file to Cavatica.

    https://docs.cavatica.org/docs/start-an-export-job-v2
    https://docs.cavatica.org/docs/get-details-of-an-export-job-v2

    The CavaticaTaskSensor is imported here to monitor the export job. This Operator 
    requires HTTP headers and passes them down to the CavaticaTaskSensor. This is
    to ensure the sensor has permissions to monitor the job in case two different
    Airflow connections are used.

    This Operator will return True when the import job has completed successfully,
    or fail the DAG if the export job fails.

    cavatica_conn_id: name of the Airflow Connection that points to Cavatica.
        type:       str
        example:    cavatica
    cavatica_headers: HTTP request headers for the Cavatica API
        type:       dict
        example:    {"Content-Type": "application/json", "X-SBG-Auth-Token": <token>}
    destination_parent_uri: Cavatica parent folder ID where the file will be imported to
        type:       str
        example:    610aa71f2f5730089b081ea4
    source_volume: name of the s3 as mounted to Cavatica (i.e., not the s3 path itself)
        type:       str
        example:    volume/folder    
    source_location: name of the s3 key to be created for the file relative to source_volume
        type:       str
        example:    /bams/example.bam (would create volume/folder/bams/example.bam)
    optional_fields: optional, other key-value pairs the Cavatica endpoint accepts
        type:       dict
        example:    {"overwrite": True, "sse_algorithm": "AES256"}


    returns the Cavatica URI of the import file
    """

    ui_color = '#e811fc'
    endpoint = '/storage/imports'

    @apply_defaults
    def __init__(self,
                 cavatica_conn_id,
                 cavatica_headers,
                 destination_parent_uri,
                 source_volume,
                 source_location,
                 optional_fields={},
                 *args,
                 **kwargs
                 ):
        super(CavaticaStorageImportOperator, self).__init__(*args, **kwargs)
        self.cavatica_conn_id = cavatica_conn_id
        self.cavatica_headers = cavatica_headers
        self.destination_parent_uri = destination_parent_uri
        self.source_volume = source_volume
        self.source_location = source_location
        self.optional_fields = optional_fields

    def execute(self, context):
        """Start import job and wait for COMPLETED from CavaticaTaskSensor."""

        payload = { # edit this
            "source": {
                "volume": self.source_volume,
                "location": self.source_location
            },
            "destination": {
                "parent": self.destination_parent_uri
            }
        }

        if self.optional_fields:
            for key in self.optional_fields.keys():
                payload[key] = self.optional_fields[key]

        api = HttpHook(method='POST', http_conn_id=self.cavatica_conn_id)
        response = api.run(endpoint=endpoint, json=payload, headers=self.cavatica_headers)
        response.raise_for_status()

        import_task_id = response.json()["id"]

        wait_for_import_success = CavaticaTaskSensor(
            task_id='wait_for_import_success',
            cavatica_task_id=import_task_id,
            cavatica_conn_id=self.cavatica_conn_id,
            cavatica_headers=self.cavatica_headers,
            endpoint=f'{endpoint}/',
            poke=10,
            timeout=3600
        )
        wait_for_import_success.execute(context)

        api = HttpHook(method='GET', http_conn_id=self.cavatica_conn_id)
        response = api.run(endpoint=f'{endpoint}/{import_task_id}', headers=self.cavatica_headers)
        response.raise_for_status()

        return response.json()["result"]["id"]
