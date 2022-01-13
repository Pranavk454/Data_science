from google.cloud import storage
from google.cloud import firestore
from google.cloud import storage
import logging
from datetime import datetime
import os, sys, tempfile


def cdp_de_firestore_load_metadata(event, context=None):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    event_file = event
    execution_time = datetime.now()
    runid = execution_time.strftime("%Y%m%d%H%M%S")
    logfile_name = runid + ".log"
    temp = tempfile.NamedTemporaryFile(suffix=".log")
    full_path_to_log_file = temp.name
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(level=logging.INFO, format='%(asctime)-25s: %(levelname)-7s: %(message)s',
                        filename=full_path_to_log_file, filemode='w')
    BUCKET_NAME = 'khc-logs-bucket'
    BLOB_NAME = 'cloud_functions/CDP-DE-FIRESTORE-DATALOAD/CDP-DE-FIRESTORE-DATALOAD_' + execution_time.strftime(
        "%Y%m%d%H%M%S") + '.log'
    BLOB_STR = full_path_to_log_file
    logging.info('Initialize Variables, Clients & Defs')
    print('Bucket: {}'.format(event_file['bucket']))
    logging.info('Bucket: {}'.format(event_file['bucket']))
    print('File: {}'.format(event_file['name']))
    logging.info('File: {}'.format(event_file['name']))

    print('Initialize Variables, Clients & defs')
    event_file_name_with_path = event_file['name']
    event_file_bucket = event_file['bucket']
    event_file_collection = str(event_file_name_with_path.split('/')[0])
    storage_client = storage.Client()
    metadata_store = firestore.Client(project="poc-dna-gcp-cdp-repo-623320")
    data_rows = []

    def batch_data(iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    print('create blob connection to the file in storage')
    logging.info('create blob connection to the file in storage')
    file_name_with_path = storage_client.get_bucket(event_file_bucket).blob(event_file_name_with_path)

    print('Fetch the data from file')
    logging.info('Fetch the data from file')
    str_data = file_name_with_path.download_as_string()
    print(f'Printing Data from file - {str_data}')
    logging.info(f'Printing Data from file - {str_data}')
    print('Convert Bytes String to UTF-8 string')
    logging.info('Convert Bytes String to UTF-8 string')
    str_data_utf8 = str(str_data, "utf-8")

    print('Convert STR to List of rows')
    logging.info('Convert STR to List of rows')
    list_rows = str_data_utf8.split("\r\n")

    def batch_data(iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    print('Fetch Headers')
    headers = list_rows[0].split(',')
    print(f'Headers: {headers}')
    logging.info(f'Headers: {headers}')
    print('Fetch Data Rows and format them to a dict')
    logging.info('Fetch Data Rows and format them to a dict')
    for row in list_rows[1:]:
        obj = {'metadata_file_bucket': str(event_file_bucket),
               'metadata_file_name_with_path': str(event_file_name_with_path)}
        row_data = row.split(',')
        for idx, item in enumerate(row_data):
            obj[headers[idx]] = item
        data_rows.append(obj)
    print(f'Processed {len(list_rows) - 1} lines.')
    print(f'Data Rows: {data_rows}')
    logging.info(f'Data Rows: {data_rows}')

    print('Start the data load to Fires')
    try:
        for batched_data in batch_data(data_rows, 499):
            batch = metadata_store.batch()
            for data_item in batched_data:
                doc_ref = metadata_store.collection(event_file_collection).document(data_item['firestore_doc_id'])
                batch.set(doc_ref, data_item)
            batch.commit()
    except Exception as e:
        print("failed with exception", e)

    print('Data Load Completed')
    logging.info('Data Load Completed')
    write_logs(BUCKET_NAME, BLOB_STR, BLOB_NAME)


def write_logs(bucket_name, blob_text, destination_blob_name):
    """Uploads the logging details file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(blob_text)