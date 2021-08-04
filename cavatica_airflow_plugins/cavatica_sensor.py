# -*- coding: utf-8 -*-
from base64 import b64decode
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

logging.basicConfig(format='%(asctime)s - %(levelname)s:%(message)s', level=logging.DEBUG)


class CavaticaTaskSensor(BaseSensorOperator):
    """Uses the Cavatica API to monitor a task.

    This sensor returns False if the task is still running and will check again
    after the defined poke_interval has passed. The sensor will return True when
    Cavatica reports the task is 'COMPLETED'. Any other status will raise
    an AirflowException (which should the parent DAG).

    The authentication provided within the cavatica_conn_id needs to be able to
    access the cavatica_task_id.

    NOTE: headers and an endpoint do not need to be passed to this sensor as they 
    are created automatically using the cavatica_conn_id. The internal methods
    _build_headers assumes the Cavatica developer token is stored in the 
    Password field of the Airflow connection. The endpoint is hardcoded as this
    sensor is only meant for getting task run information and parsing the response
    from that specific endpoint.

    cavatica_task_id: the ID assigned to a Cavatica task. eligible for Jinja2 templating
        type:       str
        example:    4c8f18a1-3596-49bf-a2a2-b5e598666435
    cavatica_conn_id: name of the Airflow Connection that points to Cavatica.
        type:       str
        example:    cavatica
    cavatica_headers: optional, HTTP request headers for the Cavatica API
        type:       dict
        example:    {"Content-Type": "application/json", "X-SBG-Auth-Token": <token>}


    returns True or False
    """

    template_fields = ['cavatica_task_id']

    @apply_defaults
    def __init__(self,
                 cavatica_task_id,
                 cavatica_conn_id,
                 cavatica_headers={},
                 *args,
                 **kwargs
                 ):
        super(CavaticaTaskSensor, self).__init__(*args, **kwargs)
        self.cavatica_task_id = cavatica_task_id
        self.cavatica_conn_id = cavatica_conn_id
        self.cavatica_headers = cavatica_headers

    def _build_headers(cavatica_conn_id):
        """Generates HTTP headers based on the Cavatica Airflow connection"""
        try:
            return {
                "Content-Type": "application/json",
                "X-SBG-Auth-Token": BaseHook.get_connection(cavatica_conn_id).get_password()
            }
        except Exception as err:
            msg = f'Unable to generate headers using the cavatica_conn_id: {err}'
            logging.error(msg)
            raise AirflowException(msg)

    def poke(self, context):
        """Check the status using the GET method.

        See the Cavatica API documentation for more info:
        https://docs.cavatica.org/docs/get-details-of-a-task
        """

        if not self.cavatica_headers:
            self.cavatica_headers = self._build_headers(self.cavatica_conn_id)

        api = HttpHook(method='GET', http_conn_id=self.cavatica_conn_id)
        response = api.run(endpoint=f'/tasks/{self.cavatica_task_id}', headers=self.cavatica_headers)
        response.check_response()

        try:
            response_json = response.json()
            status = response_json["status"].upper()
        except Exception as err:
            msg = f'Unable to parse Cavatica API response: {err}'
            logging.error(msg)
            raise AirflowException(msg)

        if status in ["QUEUED", "RUNNING"]:
            logging.info(f'{self.cavatica_task_id} is still running...')
            return False
        elif status == "COMPLETED":
            logging.info(f'{self.cavatica_task_id} finished successfully!')
            return True
        elif status == "PENDING":
            msg = f'{self.cavatica_task_id} is pending and needs to be started!'
            logging.error(msg)
            raise AirflowException(msg)
        elif status in ["ABORTED", "FAILED"]:
            msg = f'{self.cavatica_task_id} did not finish!'
            logging.error(msg)
            raise AirflowException(msg)
        else:
            msg = f'{self.cavatica_task_id} has unhandled job state "{status}", this DAG run will be failed'
            raise AirflowException(msg)