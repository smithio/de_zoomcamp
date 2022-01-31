import logging

from google.cloud import storage

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def format_to_parquet_chunks(src_file):
    dest_file = src_file.replace('.csv', '.parquet')
    writer = None
    with pv.open_csv(src_file) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pq.ParquetWriter(dest_file, next_chunk.schema)
            next_table = pa.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)