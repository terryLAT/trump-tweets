import os
import gzip
import json
import boto3
import requests
import datetime
from io import BytesIO


def main(file_name="tweets.json"):
    """
    Downloads all @realDonaldTrump tweets stored at trumptwitterarchive.com.

    Uploads them as a single JSON to S3.
    """
    # Scrape
    base_url = 'http://www.trumptwitterarchive.com/data/realdonaldtrump/{}.json'
    year_list = range(2009, datetime.date.today().year + 1)
    json_list = []
    for year in year_list:
        r = requests.get(base_url.format(year))
        data = r.json()
        data.reverse()
        json_list.append(data)
    flat_list = [item for sublist in json_list for item in sublist]
    print(f"{len(flat_list)} tweets found")

    # Write
    buffer = BytesIO()
    kwargs = dict(
        filename=file_name,
        mode='wb',
        fileobj=buffer,
        mtime=0
    )
    with gzip.GzipFile(**kwargs) as f:
        f.write(json.dumps(flat_list).encode('utf-8'))
    with open(file_name, 'wb') as outfile:
        outfile.write(buffer.getvalue())
    print(f"Wrote out gzipped JSON to {file_name}")

    # Upload
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(os.getenv("AWS_BUCKET_NAME"))
    bucket.upload_file(
        file_name,
        file_name,
        ExtraArgs={
            'ACL':'public-read',
            'ContentEncoding': 'gzip',
            'ContentType': "application/json"
        }
    )
    print("Uploaded tweets.json to S3 bucket")


if __name__ == '__main__':
    main()
