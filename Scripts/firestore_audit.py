def invoke_firestore(**context):
    from google.cloud import firestore
    import datetime
    firestore_connection_str = firestore.Client()
    dag_record = firestore_connection_str.collection('khcmc_fs_col_log_composer_run').where('dag_run_id', '==',
                                                                                            context['dag_run'].run_id)
    count_doc = 0
    for doc in dag_record.stream():
        count_doc = count_doc + 1
        docref = firestore_connection_str.collection('khcmc_fs_col_log_composer_run').document(doc.id)
        docref.update(
            {'executed_end': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'execution_status': 'SUCCESS',
             'error_details': ''})
    if count_doc == 0:
        if len(context['params'].keys()) == 0:
            context['params'] = ""
        firestore_connection_str.collection('khcmc_fs_col_log_composer_run').document().set(
            {
                'dag_name': context['dag'].dag_id,
                'dag_run_id': context['dag_run'].run_id,
                'executed_start': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'executed_end': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'execution_status': 'RUNNING',
                'error_details': '',
                'params': context['params']
            }
        )


def log_to_explorer(log_details, context):
    from google.cloud import logging
    import datetime
    log_client = logging.Client()
    logger = log_client.logger("dag_logger")

    print(log_client, context)
    now = str(datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
    logger.log_struct({"message":
        {
            'dag': context['dag_run'].dag_id,
            'dag_run_id': context['dag_run'].run_id,
            'task_id': context['task'].task_id,
            'task_run_id': context['ti'].job_id,
            'start_time': str(context['ti'].start_date.strftime("%m/%d/%Y, %H:%M:%S")),
            'params': log_details.get('params'),
            'execution_status': log_details.get('Status', 'Running'),
            'error_details': log_details.get('error_message', ''),
            'end_time': now if log_details.get('Status', None) else '',
        }
    }
    )
    logger.log_struct({"message":
        {
            'dag': context['dag_run'].dag_id,
            'dag_run_id': context['dag_run'].run_id,
            'start_time': str(context['dag_run'].start_date.strftime("%m/%d/%Y, %H:%M:%S")),
            'end_time': now if log_details.get('DAG_status') else '',
            'execution_status': log_details.get('DAG_status') if log_details.get('DAG_status') else 'Running',
            'error_details': log_details.get('error_message', '')
        }
    }
    )


def on_success_audit(context):
    from google.cloud import firestore
    import datetime
    log_to_explorer({'Status': "SUCCESS", 'DAG_status': context.get("DAG_Status")}, context)
    firestore_connection_str = firestore.Client()
    if len(context['params'].keys()) == 0:
        context['params'] = ""
    firestore_connection_str.collection('khcmc_fs_col_log_composer_run_details').document().set(
        {
            'dag_run_id': context['dag_run'].run_id,
            'task_run_id': context['ti'].job_id,
            'executed_start': context['ti'].start_date.strftime('%Y-%m-%d %H:%M:%S'),
            'executed_end': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'execution_status': 'SUCCESS',
            'error_details': "",
            'params': context['params']
        }
    )


def on_failure_audit(context):
    from google.cloud import firestore
    import datetime
    log_to_explorer({'Status': "FAILURE", 'error_message': str(context['exception']), 'DAG_status': 'FAILURE'}, context)
    firestore_connection_str = firestore.Client()
    exception = context.get('exception')
    formatted_exception = ''.join(
        traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)).strip()
    if len(context['params'].keys()) == 0:
        context['params'] = ""
    firestore_connection_str.collection('khcmc_fs_col_log_composer_run_details').document().set(
        {
            'dag_run_id': context['dag_run'].run_id,
            'task_run_id': context['ti'].job_id,
            'executed_start': context['ti'].start_date.strftime('%Y-%m-%d %H:%M:%S'),
            'executed_end': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'execution_status': 'FAILED',
            'error_details': formatted_exception,
            'params': context['params']
        }
    )
    dag_record = firestore_connection_str.collection('khcmc_fs_col_log_composer_run').where('dag_run_id', '==',
                                                                                            context['dag_run'].run_id)
    count_doc = 0
    for doc in dag_record.stream():
        count_doc = count_doc + 1
        docref = firestore_connection_str.collection('khcmc_fs_col_log_composer_run').document(doc.id)
        docref.update(
            {'executed_end': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'execution_status': 'FAILED',
             'error_details': formatted_exception})
