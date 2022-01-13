from google.cloud import bigquery
from google.cloud import firestore
import json
from google.cloud import storage
import datetime
import shutil
import io
from zipfile import ZipFile, is_zipfile
import re
from pathlib import Path
import tempfile, os, sys
from .unzip_file_in_gcs import unzip_file_in_gcs, access_secret_version


def make_audit_entry(audit_dict):
    metadata_store = firestore.Client()
    for audit_key in audit_dict.keys():
        audit_entry = audit_dict[audit_key]
        audit_entry[u'audit_datetime'] = datetime.datetime.now()
        metadata_store.collection('fs_col_bq_raw_dataload_audit').add(audit_entry)


# test
def load_file_to_raw(input_source_details, ):
    file_to_load = json.loads(str(input_source_details).replace("'", '"'))
    source = file_to_load['Source']
    datasetgroup = file_to_load['datasetgroup']
    datalevel = file_to_load['datalevel']
    af_run_id = file_to_load['af_run_id']
    bigquery_client = bigquery.Client()
    metadata_store = firestore.Client()
    row_cnt = 0
    print('Fetch the bigquery table details from Metadata')
    metadata_collection = metadata_store.collection('fs_col_dataload_orchestration')
    metadata_query = metadata_collection.where(u'datalevel', u'==', datalevel)
    metadata_query = metadata_query.where(u'active_ind', u'==', 'Y')
    metadata_query = metadata_query.where(u'source', u'==', source)
    metadata_query = metadata_query.where(u'datasetgroup', u'==', datasetgroup)
    metadata_rows = metadata_query.stream()
    audit_doc = {
        u'dag_exec_id': af_run_id,
        u'audit_datetime': '',
        u'error_details': '',
        u'load_file': '',
        u'recs_loaded_tobq': 0,
        u'raw_bq_table': '',
    }
    audit_dict = {}
    for metadata_row in metadata_rows:
        row_cnt = row_cnt + 1
        metadata = metadata_row.to_dict()
        print('metadata: ', metadata)
        print('IF Condition', metadata['add_prefix'])
        if metadata['add_prefix'] == "Y":
            rename_file(metadata)
        file_to_load_bucket = f"{metadata['gs_inbound_bucket_name']}"
        file_to_load_name_with_path = f"{metadata['gs_inbound_folder_path']}" + '/' + f"{metadata['bq_load_filename']}"
        table_name = f"{metadata['bq_projectname']}.{metadata['bq_datasetname']}.{metadata['bq_raw_table_name']}"
        folder_path = f"{metadata['gs_inbound_folder_path']}"
        path_till_folder = f"gs://{file_to_load_bucket}/{file_to_load_name_with_path}"
        ordinal_num = len(path_till_folder.split('/'))
        file_format = f"{metadata['bq_load_file_format']}"
        file_delimiter = f"{metadata['bq_load_file_delimiter']}"
        row_to_skip = f"{metadata['bq_load_file_headers_rec_to_skip']}"
        project_id = metadata['bq_projectname']
        dataset_id = metadata['bq_datasetname']
        table_id = metadata['bq_raw_table_name']
        # metadata_dict = metadata
        # storage_client = storage.Client()

        if 'bq_load_file_compression' in metadata:
            file_compression = metadata['bq_load_file_compression']
        else:
            file_compression = "NONE"
        print('Table - %s entry is fetched from metadata with dataset as: %s and project id as: %s' % (
        table_id, dataset_id, project_id))
        query_string = f'select ddl from `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES` where table_name = "{table_id}"'
        query_job = bigquery_client.query(query_string)
        for i in query_job:
            proc_result = i.ddl
            proc_result = proc_result.replace('CREATE TABLE', 'CREATE OR REPLACE EXTERNAL TABLE')
            proc_result = proc_result.replace(table_name, table_name + '_EXT')
            proc_result = ((proc_result.split('LOAD_FILENAME STRING')[0]).rstrip())[:-1] + ')'
            if file_compression != 'NONE':
                option = 'OPTIONS ( format = "%s",uris = ["%s"],field_delimiter = "%s",skip_leading_rows = %s,compression = "%s");' % (
                file_format, path_till_folder, file_delimiter, row_to_skip, file_compression)
            else:
                if file_format != 'JSON':
                    option = "OPTIONS ( format = '%s',uris = ['%s'],field_delimiter = '%s',skip_leading_rows = %s, allow_quoted_newlines = %s);" % (
                    file_format, path_till_folder, file_delimiter, row_to_skip, True)
                else:
                    option = 'OPTIONS ( format = "%s",uris = ["%s"]);' % (file_format, path_till_folder)
            proc_result = proc_result + ' ' + option
            break

        run_job = bigquery_client.query(proc_result)
        run_job.result()
        print('Got file info from External table')
        print(f'Loading Data to Table - {table_name} from External table - {table_name}_EXT')

        print('Now truncating RAW table and insert data from External table.')

        query_string1 = 'TRUNCATE TABLE ' + table_name + ';'
        query_string1 += """
                        INSERT INTO
                        """
        query_string1 += ' ' + table_name
        query_string1 += " SELECT *,split(_FILE_NAME,'/')[ordinal(%s)] LOAD_FILENAME FROM" % (ordinal_num)
        query_string1 += ' ' + table_name + '_EXT;'
        query_string1 += 'DROP TABLE ' + table_name + '_EXT;'

        try:
            load_job = bigquery_client.query(query_string1)
            load_job.result()

        except Exception as e:
            query_string3 = f"SELECT LOAD_FILENAME,COUNT(LOAD_FILENAME) AS LOAD_FILENAME_COUNT FROM {table_name} GROUP BY LOAD_FILENAME"
            run_job = bigquery_client.query(query_string3)
            data = run_job.result()
            for i in list(data):
                file_path = f"gs://{file_to_load_bucket}/{metadata['gs_inbound_folder_path']}/{i.LOAD_FILENAME}"
                audit_entry = dict(audit_doc)
                audit_entry[u'load_file'] = i.LOAD_FILENAME
                audit_entry[u'recs_loaded_tobq'] = i.LOAD_FILENAME_COUNT
                audit_entry[u'raw_bq_table'] = table_name
                audit_entry[u'error_details'] = f"Failed to load data with exception {e}"
                audit_dict[file_path] = audit_entry
            make_audit_entry(audit_dict)
            sys.exit(1)
        query_string3 = f"SELECT LOAD_FILENAME,COUNT(LOAD_FILENAME) AS LOAD_FILENAME_COUNT FROM {table_name} GROUP BY LOAD_FILENAME"
        run_job = bigquery_client.query(query_string3)
        data = run_job.result()
        for i in list(data):
            file_path = f"gs://{file_to_load_bucket}/{metadata['gs_inbound_folder_path']}/{i.LOAD_FILENAME}"
            audit_entry = dict(audit_doc)
            audit_entry[u'load_file'] = i.LOAD_FILENAME
            audit_entry[u'recs_loaded_tobq'] = i.LOAD_FILENAME_COUNT
            audit_entry[u'raw_bq_table'] = table_name
            audit_dict[file_path] = audit_entry
        make_audit_entry(audit_dict)
        print('Fetch the Rows load')
        destination_table = bigquery_client.get_table(table_name)
        print(f"Loaded Table: {table_name} with {destination_table.num_rows} rows.")
        # delete_file_from_in(metadata_row)

    if row_cnt >= 1:
        print(f'Loaded {row_cnt} tables based on Bucket Trigger Event')
    else:
        print('No Matching Metadata Found for the Bucket Trigger Event')


def rename_file(metadata_row):
    print('Entered into IF condition')
    """Rename file in GCP bucket."""
    client = storage.Client()
    BUCKET_NAMEONE = f"{metadata_row['gs_inbound_bucket_name']}"
    FOLDER_NAMEONE = f"{metadata_row['gs_inbound_folder_path']}"
    prefixONE = FOLDER_NAMEONE + '/'
    bucketONE = client.get_bucket(BUCKET_NAMEONE)

    prefix_name = metadata_row['datafile_name_prefix']
    search_filename = metadata_row['datafile_name']
    sblobs = client.list_blobs(bucketONE, prefix=prefixONE, delimiter=None)
    print('prefixONE: ', prefixONE)
    print('bucketONE: ', bucketONE)
    print('prefixxNAME: ', prefix_name)
    print('seachfilename #####: ', search_filename)
    print('blobs #####: ', sblobs)

    for source_blob in sblobs:
        find_delimiter = search_filename.replace('*', '.*')
        blob_name = source_blob.name.split('/')[-1]
        print('blob_name #####: ', blob_name)
        print('find_delimiter #####: ', find_delimiter)
        if re.match(find_delimiter, blob_name):
            new_blob_name = prefix_name + blob_name
            newblobFINAL = prefixONE + new_blob_name
            bucketONE.rename_blob(source_blob, newblobFINAL)


# As we are loading all files from all buckets with a given source the archival should also be done in the same iterations
def copy_inbound_to_archive(input_source_details):
    file_to_load = json.loads(str(input_source_details).replace("'", '"'))
    source = file_to_load['Source']
    datasetgroup = file_to_load['datasetgroup']
    datalevel = file_to_load['datalevel']
    print('Fetch the bigquery table details from Metadata')
    metadata_store = firestore.Client()
    metadata_collection = metadata_store.collection('fs_col_dataload_orchestration')
    metadata_query = metadata_collection.where(u'datalevel', u'==', datalevel)
    metadata_query = metadata_query.where(u'active_ind', u'==', 'Y')
    metadata_query = metadata_query.where(u'source', u'==', source)
    metadata_query = metadata_query.where(u'datasetgroup', u'==', datasetgroup)
    metadata_rows = metadata_query.stream()
    client = storage.Client()
    for metadata_row in metadata_rows:
        prefix = f"{metadata_row.to_dict()['gs_archive_filename_prefix']}"
        suffix = f"{metadata_row.to_dict()['gs_archive_filename_suffix']}"
        inbound_bucket = f"{metadata_row.to_dict()['gs_inbound_bucket_name']}"
        inbound_folder = f"{metadata_row.to_dict()['gs_inbound_folder_path']}"
        archive_bucket = f"{metadata_row.to_dict()['gs_archive_bucket_name']}"
        delimiter = f"{metadata_row.to_dict()['datafile_name']}"
        # prefix = f"{metadata_row.to_dict()['gs_archive_filename_prefix']}"
        # suffix = f"{metadata_row.to_dict()['gs_archive_filename_suffix']}"
        archive_folder = f"{metadata_row.to_dict()['gs_archive_folder_path']}"
        print(f'Archiving files to {archive_folder}')
        metadata_dict = metadata_row.to_dict()
        if metadata_row.to_dict()['zipped_file'] == "Y":
            gs_bucket_name = metadata_dict['gs_inbound_bucket_name']
            gs_file_name = metadata_dict['zippedfile_name']
            gs_folder_path = metadata_dict['gs_inbound_folder_path']
            zipped_format = metadata_dict['zipped_format']
            access_key1 = metadata_dict['access_key']
            search_prefix = gs_folder_path + '/'
            source_bucket = client.get_bucket(gs_bucket_name)
            blobs = list(source_bucket.list_blobs(prefix=search_prefix))
            for source_blob in blobs:
                find_delimiter = gs_file_name.replace('*', '.*')
                blob_name = source_blob.name.split('/')[-1]
                if re.match(find_delimiter, blob_name):
                    file_to_unzip = source_blob.name.split('/')[-1]
                    if file_to_unzip != '':
                        print(f'unzipping file {file_to_unzip}')
                        complete_path = gs_folder_path + '/' + file_to_unzip
                        unzip_file_in_gcs(gs_bucket_name, complete_path, zipped_format, access_key1)
        copy_with_prefix_suffix(client, inbound_bucket, archive_bucket, archive_folder, delimiter, prefix, suffix,
                                inbound_folder)


def copy_with_prefix_suffix(client, sourceBucket, destinationBucket, archive_folder, delimiter_to_search,
                            prefix_to_append, suffix, inbound_folder):
    source_bucket = client.get_bucket(sourceBucket)
    search_prefix = inbound_folder + '/'
    blobs = list(source_bucket.list_blobs(prefix=search_prefix))
    if suffix != '':
        suffix = str(suffix).replace('<YYYY>', datetime.datetime.now().strftime("%Y")).replace('<YY>',
                                                                                               datetime.datetime.now().strftime(
                                                                                                   "%Y")[-2]).replace(
            '<MM>', datetime.datetime.now().strftime("%m")).replace('<DD>',
                                                                    datetime.datetime.now().strftime("%d")).replace(
            '<HH24>', datetime.datetime.now().strftime("%H")).replace('<HH>',
                                                                      datetime.datetime.now().strftime("%H")).replace(
            '<MI>', datetime.datetime.now().strftime("%M")).replace('<SS>',
                                                                    datetime.datetime.now().strftime("%S")).replace(
            '<FFFFFF>', datetime.datetime.now().strftime("%f"))
    else:
        suffix = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

    for source_blob in blobs:
        file_name = source_blob.name.split('/')[-1]
        print("found with search  ", delimiter_to_search, file_name, source_blob.size)
        if source_blob.name != search_prefix:
            extn = source_blob.name.split('.')[-1]
            '''if extn in ['zip','rar']:
                extracted_gcs_file_list = unzip_file(client, source_bucket,source_blob, file_name)				
                #source_bucket.copy_blob(source_blob, source_bucket, destinationObject)
                for file_to_upload in extracted_gcs_file_list:
                    print("Uploading extracted  to bucket ",file_to_upload)
                    destination_bucket = client.get_bucket(destinationBucket)
                    blob_name = file_to_upload.split('/')[-1]
                    destinationObject=f"{archive_folder}/{prefix_to_append}_{blob_name.split('.')[0]}_{datetime.datetime.now().strftime( '%Y%m%d%H%M%S' )}.{blob_name.split('.')[1]}"
                    print("Moved to archive ", destinationObject)
                    source_bucket.copy_blob(source_bucket.blob(file_to_upload), destination_bucket, destinationObject)                        
                source_blob.delete()
            else: '''
            find_delimiter = delimiter_to_search.replace('*', '.*')
            print('Matched file to be copied via regex')
            print('source_blob.name:', source_blob.name)
            print('search_prefix:', search_prefix)
            print("found with in", delimiter_to_search, source_blob.name.split('/')[-1])
            destination_bucket = client.get_bucket(destinationBucket)
            blob_name = source_blob.name.split('/')[-1]
            print('find_del', find_delimiter, blob_name)
            if re.match(find_delimiter, blob_name):
                print('Matched files to be copied via regex')
                blob_name = blob_name.split('.')
                destinationObject = f"{archive_folder}/{prefix_to_append}_{blob_name[0]}_{suffix}.{blob_name[1]}"
                print(destinationObject)
                new_blob = source_bucket.copy_blob(source_blob, destination_bucket, destinationObject)


def delete_file_from_in(metadata_row):
    client = storage.Client()
    inbound_bucket = f"{metadata_row.to_dict()['gs_inbound_bucket_name']}"
    inbound_folder = f"{metadata_row.to_dict()['gs_inbound_folder_path']}"
    delimiter = f"{metadata_row.to_dict()['bq_load_filename']}"
    search_prefix = inbound_folder + '/'
    source_bucket = client.get_bucket(inbound_bucket)
    print(f'Files will be deletd from {source_bucket}')
    blobs = list(source_bucket.list_blobs(prefix=search_prefix))
    for source_blob in blobs:
        find_delimiter = delimiter.replace('*', '.*')
        blob_name = source_blob.name.split('/')[-1]
        if re.match(find_delimiter, blob_name):
            print('Matched files to be delete via regex')
            file_to_delete = source_blob.name.split('/')[-1]
            if file_to_delete != '':
                print(f'Deleting file {file_to_delete}')
                source_blob.delete()


def delete_files_from_inbound(input_source_details):
    file_to_load = json.loads(str(input_source_details).replace("'", '"'))
    source = file_to_load['Source']
    datasetgroup = file_to_load['datasetgroup']
    datalevel = file_to_load['datalevel']
    print('Fetch the bigquery table details from Metadata')
    metadata_store = firestore.Client()
    metadata_collection = metadata_store.collection('fs_col_dataload_orchestration')
    metadata_query = metadata_collection.where(u'datalevel', u'==', datalevel)
    metadata_query = metadata_query.where(u'active_ind', u'==', 'Y')
    metadata_query = metadata_query.where(u'source', u'==', source)
    metadata_query = metadata_query.where(u'datasetgroup', u'==', datasetgroup)
    metadata_rows = metadata_query.stream()
    client = storage.Client()
    for metadata_row in metadata_rows:
        inbound_bucket = f"{metadata_row.to_dict()['gs_inbound_bucket_name']}"
        inbound_folder = f"{metadata_row.to_dict()['gs_inbound_folder_path']}"
        delimiter = f"{metadata_row.to_dict()['bq_load_filename']}"
        search_prefix = inbound_folder + '/'
        source_bucket = client.get_bucket(inbound_bucket)
        print(f'Files will be deletd from {source_bucket}')
        blobs = list(source_bucket.list_blobs(prefix=search_prefix))
        for source_blob in blobs:
            find_delimiter = delimiter.replace('*', '.*')
            blob_name = source_blob.name.split('/')[-1]
            if re.match(find_delimiter, blob_name):
                print('Matched files to be delete via regex')
                file_to_delete = source_blob.name.split('/')[-1]
                if file_to_delete != '':
                    print(f'Deleting file {file_to_delete}')
                    source_blob.delete()