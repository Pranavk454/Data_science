from google.cloud import bigquery
from google.cloud import firestore
import json
from google.cloud import storage
import datetime
import shutil
import io
import re
import tempfile, os, sys


'''
gcp_pranav_archive : archive name
gcp_test_csv : inbound bucket
gcp_dataload_input : metadata collection name
'''

def inbound_to_archive(input_source_details):
    #file_to_load = json.loads(str(input_source_details))
    #collection_name = file_to_load["collection"]
    collection_name = 'gcp_dataload_input'

    print("Fetching Bucket details from metadata")
    metadata_store = firestore.Client()
    metadata_collection = metadata_store.collection(collection_name)
    metadata_query = metadata_collection.where(u'owner', u'==', u'pranav')
    metadata_rows = metadata_query.stream()
    bucket_client = storage.Client()

    for metadata_row in metadata_rows:
        metadata_dict = metadata_row.to_dict()
        prefix = metadata_dict["prefix"]
        inbound_bucket = metadata_dict["inbound_bucket"]
        inbound_folder = metadata_dict["inbound_folder"]
        archive_bucket = metadata_dict["archive_bucket"]
        archive_folder = metadata_dict["archive_folder"]

        print(f"Archiving file to {archive_folder}")

        copy_with_prefix(bucket_client,inbound_bucket,inbound_folder,archive_bucket,archive_folder,prefix)

def copy_with_prefix(bucket_client,source_bucket,source_folder,destination_bucket,destination_folder,prefix):
    source_bucket = bucket_client.get_bucket(source_bucket)
    search_prefix = source_folder + '/'
    blobs = list(source_bucket.list_blobs(prefix = search_prefix))
    for source_blob in blobs:
        file_name = source_blob.name.split('/')[-1]
        print("File Found",file_name, source_blob.size)

        if source_blob.name != search_prefix:
            extn = source_blob.name.split('.')[-1]
            destination_bucket = bucket_client.get_bucket(destination_bucket)
            blob_name = file_name.split('.')
            destination_object = f"{destination_folder}/{prefix}_{blob_name[0]}.{blob_name[1]}"
            print("Destination Object",destination_object)
            new_blob = source_bucket.copy_blob(source_blob, destination_bucket, destination_object)



def load_to_table(input_source_details):
    collection_name = 'gcp_dataload_input'

    print("Fetching Bucket details from metadata")
    metadata_store = firestore.Client()
    metadata_collection = metadata_store.collection(collection_name)
    metadata_query = metadata_collection.where(u'owner', u'==', u'pranav')
    metadata_rows = metadata_query.stream()
    bucket_client = storage.Client()

    for metadata_row in metadata_rows:
        metadata_dict = metadata_row.to_dict()
        prefix = metadata_dict["prefix"]
        inbound_bucket = metadata_dict["inbound_bucket"]
        inbound_folder = metadata_dict["inbound_folder"]
        dataset_id = metadata_dict["bq_datasetname"]
        table_id = metadata_dict["bq_table_name"]
        table_id = f"poc-dna-gcp-cdp-623320.{dataset_id}.{table_id}"

        bigquery_client = bigquery.Client()
        source_bucket = bucket_client.get_bucket(inbound_bucket)
        search_prefix = inbound_folder + '/'
        blobs = list(source_bucket.list_blobs(prefix=search_prefix))
        for source_blob in blobs:
            file_name = source_blob.name.split('/')[-1]
            print("File Found", file_name, source_blob.size)
            if source_blob.name != search_prefix:
                #table_id = f"poc-dna-gcp-cdp-623320.{dataset_id}.{file_name.split('.')[0]}"
                test_table = f"poc-dna-gcp-cdp-623320.{dataset_id}.test_table"
                cloud_file_uri = f"gs://{inbound_bucket}/{source_blob.name}"

                print(f"Creating Table {test_table} from {cloud_file_uri}")

                job_config = bigquery.LoadJobConfig()
                job_config.autodetect = True
                job_config.source_format = bigquery.SourceFormat.CSV

                load_job = bigquery_client.load_table_from_uri(cloud_file_uri,test_table, job_config=job_config)
                load_job.result()  #Important waiting for table to create
                print("Data Loaded in Table")
                destination_table = bigquery_client.get_table(test_table)
                print(f"Loaded Table: {test_table} with {destination_table.num_rows} rows.")
                name = f"'{file_name}'" okay lets try to modify this shit

                query_string1 = 'TRUNCATE TABLE ' + table_id + ';'
                query_string1 += """
                                INSERT INTO
                                """
                query_string1 += ' ' + table_id
                query_string1 += " SELECT *,"+name + " FROM"
                query_string1 += ' '+test_table+';'
                query_string1 += 'DROP TABLE ' + test_table+';'


                load_job = bigquery_client.query(query_string1)
                load_job.result()




def delete_files_inbound(input_source_details):
    collection_name = 'gcp_dataload_input'

    print("Fetching Bucket details from metadata")
    metadata_store = firestore.Client()
    metadata_collection = metadata_store.collection(collection_name)
    metadata_query = metadata_collection.where(u'owner', u'==', u'pranav')
    metadata_rows = metadata_query.stream()
    bucket_client = storage.Client()

    for metadata_row in metadata_rows:
        metadata_dict = metadata_row.to_dict()
        inbound_bucket = metadata_dict["inbound_bucket"]
        inbound_folder = metadata_dict["inbound_folder"]
        source_bucket = bucket_client.get_bucket(inbound_bucket)
        search_prefix = inbound_folder + '/'
        blobs = list(source_bucket.list_blobs(prefix=search_prefix))
        for source_blob in blobs:
            file_name = source_blob.name.split('/')[-1]
            print("File Found", file_name, source_blob.size)
            if source_blob.name != search_prefix:
                source_blob.delete()







