from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import pandas as pd
import requests
from requests.exceptions import HTTPError
from sklearn.preprocessing import StandardScaler
from io import BytesIO
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['aquil.codes@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#Intitlaizing Dag and grabbing data on Wednesday and Friday when it is updated
@dag(default_args=default_args, schedule_interval='0 1 * * 3,5', start_date=days_ago(1), catchup=False)
def healthcare_data_processing_flow():
    
    @task
    def fetch_data():
        raw_df = pd.DataFrame()
        offset = 0
        # THE API restricts to only 1000 records per call
        batch_size = 1000
        continue_fetching = True
        api_url = "https://healthdata.gov/resource/g62h-syeh.json"
        
        while continue_fetching:
            try:
                url = f"{api_url}?$offset={offset}"
                logging.info(f"Retrieving data with offset {offset}")
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                if not data:
                    continue_fetching = False
                    logging.info("All data collected.")
                else:
                    current_df = pd.DataFrame(data)
                    raw_df = pd.concat([raw_df, current_df], ignore_index=True)
                    offset += batch_size
            except HTTPError as http_err:
                logging.error(f"HTTP error occurred: {http_err}")
                raise
            except Exception as err:
                logging.error(f"Other error occurred: {err}")
                raise
        return raw_df

    @task
    def transform_data(raw_df):
        if raw_df.empty:
            logging.info("No data was collected.")
            return None

        logging.info("Begin data processing")
        time_transformed = datetime.now()
        scaler = StandardScaler()
        keeping_cols = [
            'state', 'date', 'inpatient_beds', 'inpatient_beds_used', 'staffed_adult_icu_bed_occupancy',
            'inpatient_beds_used_covid', 'inpatient_beds_used_covid_coverage', 'critical_staffing_shortage_today_yes',
            'critical_staffing_shortage_today_no', 'hospital_onset_covid', 'previous_day_admission_adult_covid_confirmed',
            'total_staffed_adult_icu_beds', 'hospital_onset_covid_coverage', 'total_patients_hospitalized_confirmed_influenza_and_covid',
            'total_patients_hospitalized_confirmed_influenza', 'deaths_covid', 'previous_day_deaths_influenza'
        ]
        
        # Define colums for conversion
        numeric_cols = [
            'inpatient_beds', 'inpatient_beds_used', 'staffed_adult_icu_bed_occupancy', 'inpatient_beds_used_covid',
            'inpatient_beds_used_covid_coverage', 'critical_staffing_shortage_today_yes', 'critical_staffing_shortage_today_no',
            'hospital_onset_covid', 'previous_day_admission_adult_covid_confirmed', 'total_staffed_adult_icu_beds',
            'hospital_onset_covid_coverage', 'total_patients_hospitalized_confirmed_influenza_and_covid',
            'total_patients_hospitalized_confirmed_influenza', 'deaths_covid', 'previous_day_deaths_influenza'
        ]
        
        # Minimal cleaning and deleting cols also protects against new columns being added unexpectedly
        cleaned = raw_df[keeping_cols].dropna(how='all') # Drop rows where all values are NaN

        # Convert fields ensures datatypes and effectively onehot encodes a few of the columns
        cleaned['date'] = pd.to_datetime(cleaned['date'])
        cleaned[numeric_cols] = cleaned[numeric_cols].apply(pd.to_numeric, errors='coerce')

        # State-wise median filling and normalization
        cleaned[numeric_cols] = cleaned.groupby('state')[numeric_cols].transform(lambda x: x.fillna(x.median()))
        cleaned['normalized_inpatient_beds_used'] = cleaned.groupby('state')['inpatient_beds_used'].transform(
            lambda x: scaler.fit_transform(x.values.reshape(-1, 1)).flatten()
        )
        
        # Differencing for time series data
        cleaned.set_index('date', inplace=True)
        cleaned.sort_index(inplace=True)
        cleaned['inpatient_beds_used_diff'] = cleaned.groupby('state')['inpatient_beds_used'].diff(periods=14)        
        cleaned.reset_index(inplace=True)
        # Adding time loaded for data lineage
        cleaned['time_transformed'] = time_transformed  


        return cleaned
    
    #Deprecated idea but kept around to show evolution of thought process
    # def bucket_ops(bucket_name,s3_session):
    #     buckets = s3_session.buckets.all()
    #     if bucket_name in [bucket.name for bucket in buckets]:
    #         logging.info(f"Bucket '{bucket_name}' already exists.")
    #         return True

    #     try:
    #         # Create the bucket
    #         s3_session.create_bucket(Bucket=bucket_name)
    #         print(f"Bucket {bucket_name} created successfully.")
    #         return True
    #     except ClientError as e:
    #         print(f"Error creating bucket: {e}")
    #         return False



    @task
    def convert_and_upload(cleaned_df):
        if cleaned_df is not None:
            #convert to parquet
            logging.info("Converting to parquet")
            parquet_buffer = BytesIO()
            cleaned_df.to_parquet(parquet_buffer,index=False)
            parquet_buffer.seek(0)

            #send to simulated S3
            logging.info("Uploading to s3")
            s3_session = boto3.resource(
                's3',
                region_name='us-east-1',
                endpoint_url='http://localhost:4566',
                aws_access_key_id='dummykey',
                aws_secret_access_key='dummysecret',
                use_ssl=False
            )
            #if bucket_ops("Hire_aquil_bucket",s3_session=s3_session):
            upload_time = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_key = f"Healthcare_data_upload_{upload_time}.parquet"
            s3_session.Bucket("Hire_aquil_bucket").put_object(Key=file_key, Body=parquet_buffer)
            logging.info("file upload to s3 successful")


    # Task dependencies
    raw_data = fetch_data()
    cleaned_data = transform_data(raw_data)
    convert_and_upload(cleaned_data)

dag = healthcare_data_processing_flow()