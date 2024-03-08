from google.cloud import storage
import os


#set the credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\MyBuild\GCP\pulsebyttedatawork-b1df70745eae.json" #download the json file


#create the bucket
def create_bucket(bucket_name, storage_class = 'STANDARD', location = 'us-central1'):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = storage_class

    bucket = storage_client.create_bucket(bucket, location=location)

    return f'Bucket {bucket.name} created'

create_bucket('my-valid-bronze-bucket-name', 'STANDARD', 'us-central1')


