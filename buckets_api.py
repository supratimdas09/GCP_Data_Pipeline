import os
from google.cloud import storage
import requests
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
import logging

# env setup
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\MyBuild\GCP\pulsebyttedatawork-b1df70745eae.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def store_api_data(api_url, bucket_name, blob_name):

    # check url, the url is response or not
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            print("API Request Successful")
            data = response.json()
        else:
            print("Error", response.status_code, response.text)
            raise Exception("API Request Failed")
        
        #save the file as csv
        df = pd.DataFrame(data)

        csv_data = df.to_csv(index=False)

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(blob_name) # blob is the file name

        # upload the file
        blob.upload_from_string(csv_data)

        print(f'Data Store As CSV file: {blob.name}')

    except Exception as e:
        print("Error:", e)


#schedule
def schedule_data_upload():

    scheduler = BlockingScheduler()

    scheduler.add_job(scheduled_upload, 'cron', day_of_week='mon', hour=12)

    logger.info("Scheduler started. Uploading data at scheduled intervals.")
    scheduler.start()



def scheduled_upload():
    api_url = "https://fakestoreapi.com/products"
    bucket_name = "my-valid-bronze-bucket-name"
    blob_name = "e-commerce-data.csv"
    store_api_data(api_url, bucket_name, blob_name)



# store_api_data(api_url, bucket_name, blob_name)
if __name__ == "__main__":
    scheduled_upload()  
    schedule_data_upload()