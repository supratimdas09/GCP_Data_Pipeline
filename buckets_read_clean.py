import gcsfs
import pandas as pd
import os 



os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\MyBuild\GCP\pulsebyttedatawork-b1df70745eae.json"


fs = gcsfs.GCSFileSystem(project="pulsebyttedatawork")
file_path = f"gs://{"my-valid-bronze-bucket-name"}/{"e-commerce-data.csv"}"

# df = pd.read_csv(file_path, storage_options={'project': 'pulsebyttedatawork'}, chunksize=10000)
# for i in df:
#     print(i)



# def separate_rating(chunk):

#     chunk[['count','rating']] = chunk['']


def data_read_and_process(bucket_name, file_name, chunksize):
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\MyBuild\GCP\pulsebyttedatawork-b1df70745eae.json"
    fs = gcsfs.GCSFileSystem(project= "pulsebyttedatawork")
    file_path = f"gs://{"my-valid-bronze-bucket-name"}/{"e-commerce-data.csv"}"


    processed_data = []

    df = pd.read_csv(file_path, storage_options={'project': 'pulsebyttedatawork'}, chunksize=10000)
    # for i in df:
    #     print(i)
    
    for chunk in df:
        chunk[['rate','count']] = chunk['rating'].apply(lambda x: pd.Series(eval(x)))
        processed_data.append(chunk)

    final_df = pd.concat(processed_data, ignore_index=True)

    final_df.drop('rating', axis=1, inplace=True)

    #save file into silver
    output_bucket = "silver-bucket-pulse"
    output_file_name = "final-e-commerce-data.csv"
    output_path = f"gs://{output_bucket}/{output_file_name}"
    final_df.to_csv(output_path, index=False, storage_options={'project': 'pulsebyttedatawork'})
    print(f"Data saved to {output_path}")

    return final_df
        
    



bucket_name = "my-valid-bronze-bucket-name"
file_name =  "e-commerce-data.csv"
chunksize=10000

df = data_read_and_process(bucket_name, file_name, chunksize)

print(df)