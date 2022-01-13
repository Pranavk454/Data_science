def load_file_to_raw(file_to_load_str):
    from google.cloud import bigquery
    from google.cloud import firestore
    import json
    print(f'file_to_load value passed in : {file_to_load_str}')
    file_to_load = json.loads(str(file_to_load_str).replace("'", '"'))
    print(f'file_to_load value trasformed to dict : {file_to_load}')
    print('Initialize Variables, Clients & Defs')
    bigquery_client = bigquery.Client()
    metadata_store = firestore.Client()
    file_to_load_name_with_path = file_to_load['name']
    file_to_load_bucket = file_to_load['bucket']
    file_to_load_path = '/'.join([str(elem) for elem in file_to_load_name_with_path.split('/')[:-1]])
    file_to_load_name = str(file_to_load_name_with_path.split('/')[-1])
    file_to_load_collection = str(file_to_load_name_with_path.split('/')[0])
    print(
        f'Event Triggered Bucket:{file_to_load_bucket}, Folder Path:{file_to_load_path} & File Name: {file_to_load_name}')
    row_cnt = 0

    print('Fetch the bigquery table details from Metadata')
    metadata_collection = metadata_store.collection(file_to_load_collection)
    metadata_query = metadata_collection.where(u'data_level', u'==', '1')
    metadata_query = metadata_query.where(u'active_ind', u'==', 'y')
    metadata_query = metadata_query.where(u'bucketname', u'==', file_to_load_bucket)
    metadata_query = metadata_query.where(u'folder_path', u'==', file_to_load_path)
    metadata_query = metadata_query.where(u'file_name', u'==', file_to_load_name)
    metadata_rows = metadata_query.stream()
    for metadata_row in metadata_rows:
        row_cnt = row_cnt + 1
        table_name = f"{metadata_row.to_dict()['bigquery_db']}.{metadata_row.to_dict()['bigquery_dataset']}.{metadata_row.to_dict()['bigquery_table']}"
        cloudstroage_file_uri = f"gs://{file_to_load_bucket}/{file_to_load_name_with_path}"
        print(f'Loading Data to Table - {table_name} from Cloud Storage File - {cloudstroage_file_uri}')

        print('Set Big Query Job Config')
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = 'WRITE_TRUNCATE'

        print('Create the Big Query Job')
        load_job = bigquery_client.load_table_from_uri(cloudstroage_file_uri, table_name, job_config=job_config)

        print('Data load to Big Query Table')
        load_job.result()

        print('Fetch the Rows load')
        destination_table = bigquery_client.get_table(table_name)
        print(f"Loaded Table: {table_name} with {destination_table.num_rows} rows.")

    if row_cnt >= 1:
        print(f'Loaded {row_cnt} tables based on Bucket Trigger Event')
    else:
        print('No Matching Metadata Found for the Bucket Trigger Event')