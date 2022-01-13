from __future__ import print_function
import datetime
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators import email_operator
from scripts.python_scripts.load_file_to_raw import load_file_to_raw
from scripts.python_scripts.firestore_audit import invoke_firestore
from scripts.python_scripts.firestore_audit import on_success_audit
from scripts.python_scripts.firestore_audit import on_failure_audit
from airflow.exceptions import AirflowException
import traceback

# from airflow.models import Variable

default_dag_args = {
    'start_date': datetime.datetime(2021, 9, 20),
    'retries': 0
}

with models.DAG(
        'dag_customers_data_load',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    dag.doc_md = __doc__
    start_airflow = python_operator.PythonOperator(
        task_id="start_airflow",
        provide_context=True,
        python_callable=invoke_firestore,
        on_failure_callback=on_failure_audit,
        on_success_callback=on_success_audit,
    )
    start_airflow.doc_md = "Starts Audit log"

    # Ingest data from GCP Bucket to BigQuery Raw Table
    ingest_file_to_raw_table = python_operator.PythonOperator(
        task_id='ingest_file_to_raw_table',
        python_callable=load_file_to_raw,
        op_kwargs={
            'file_to_load_str': '{"bucket": "{{ dag_run.conf["bucket"] }}","name":"{{ dag_run.conf["name"] }}"}'},
        on_failure_callback=on_failure_audit,
        on_success_callback=on_success_audit,
    )

    jaffle_shop_load_customers = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='jaffle_shop_run_all_models',
        name='jaffle_shop_run_all_models',
        cmds=['python'],
        arguments=['dbt_run_python.py', 'jaffle_shop', 'jaffle_shop', '{{run_id}}', 'doc_generate_yes'],
        namespace='default',
        image='us-east1-docker.pkg.dev/poc-dna-gcp-cdp-623320/quickstart-docker-repo/quickstart-image:latest',
        image_pull_policy='Always',
        on_failure_callback=on_failure_audit,
        on_success_callback=on_success_audit,
    )

    end_airflow = python_operator.PythonOperator(
        task_id="end_airflow",
        provide_context=True,
        python_callable=invoke_firestore,
        on_failure_callback=on_failure_audit,
        on_success_callback=on_success_audit,
    )
    end_airflow.doc_md = "Audit log end"

    # Define the order in which the tasks complete by using the >> and <<
    start_airflow >> ingest_file_to_raw_table
    ingest_file_to_raw_table >> jaffle_shop_load_customers
    jaffle_shop_load_customers >> end_airflow