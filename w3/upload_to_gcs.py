import os
import requests
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "maverick-nyc-taxi-24")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def main():
    for i in range(12):
        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]

        service = "green"
        file_name = f"green_tripdata_2022-{month}.parquet"
        file_path = f"./data/{file_name}"

        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_path)
        print(f"GCS: {service}/{file_name}")


if __name__ == "__main__":
    main()
