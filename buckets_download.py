from google.cloud import storage
import os

# set up enviorment 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\MyBuild\GCP\pulsebyttedatawork-b1df70745eae.json"


def download_data(bucket_name, file_name, destination_file_name):

    storage_client = storage.Client()

    # select the bucket
    bucket = storage_client.bucket(bucket_name)

    # select the file
    blob = bucket.blob(file_name)

    # download the file
    blob.download_to_filename(destination_file_name)

    return True


bucket_name = 'my-valid-bronze-bucket-name'
file_name = "e-commerce-data.csv"
destination_file_name = "D:\MyBuild\GCP\e-commerce-data.csv"

if(download_data(bucket_name, file_name, destination_file_name)):
    print("Download Complete")
else:
    print("Not Downloaded")