from google.cloud import storage
import os


#set the credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\MyBuild\GCP\pulsebyttedatawork-b1df70745eae.json"


#upload the file
def upload_cs_file(bucket_name, source_file_name, destination_file_name):

    storage_client = storage.Client()

    # select the bucket
    bucket = storage_client.bucket(bucket_name)

    # file location 
    blob = bucket.blob(destination_file_name)

    # select the file for upload
    blob.upload_from_filename(source_file_name)

    return True

upload_cs_file('my-valid-bronze-bucket-name','D:\MyBuild\GCP\pulsebyttedatawork-b1df70745eae.json','live_laptop.xlsx')
