import datetime
from airflow import models
from airflow.operators import python_operator
from airflow.operators import bash_operator
from scripts.python_scripts.req_func_load import inbound_to_archive
from scripts.python_scripts.req_func_load import load_to_table
from scripts.python_scripts.req_func_load import delete_files_inbound


default_dag_args = {
    'start_date': datetime.datetime(2022, 1, 1),
    'retries' : 0
}

with models.DAG(
        'data_load_pranav',
        schedule_interval=None,
    default_args=default_dag_args) as dag:
    dag.doc_md = __doc__

    start_airflow = bash_operator.BashOperator(
        task_id = 'Printing_date',
        depends_on_past = False,
        bash_command = 'date',
        dag = dag,
    )

    start_airflow.doc_md = "Starting Airflow "

    #Copying File to Archive
    ingest_archive = python_operator.PythonOperator(
        task_id='ingest_archive',
        python_callable=inbound_to_archive,
        op_kwargs={"input_source_details":"hello"},
    )

    # Loading to Bigtable
    ingest_table = python_operator.PythonOperator(
        task_id='ingest_table',
        python_callable=load_to_table,
        op_kwargs={"input_source_details": "hello"},
    )

    #Deleting inbound file
    delete_file = python_operator.PythonOperator(
        task_id='delete_file',
        python_callable=delete_files_inbound,
        op_kwargs={"input_source_details": "hello"},
    )


    end_airflow = bash_operator.BashOperator(
        task_id = 'Printing_End_Time',
        depends_on_past = False,
        bash_command= 'date',
        dag=dag
    )

    end_airflow.doc_md="Airflow Stopped"

    start_airflow >> ingest_archive >> ingest_table >> delete_file >> end_airflow