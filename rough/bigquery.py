from google.cloud import bigquery
from google.cloud import storage


def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    bigquery_client = bigquery.Client()
    table_name = "csv_load.sample"
    cloudstroage_file_uri = "gs://gcp_test_csv/airtravel.csv"
    # print(f'Loading Data to Table - {table_name} from Cloud Storage File - {cloudstroage_file_uri}')

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


