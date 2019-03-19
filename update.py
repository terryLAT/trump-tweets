import os
import gzip
import json
import boto3
import requests
import datetime
from io import BytesIO
from bs4 import BeautifulSoup


def main(file_name='bogut.json'):
    """
    Downloads all @realDonaldTrump tweets stored at trumptwitterarchive.com.

    Uploads them as a single JSON to S3.
    """
    # Collect and parse page
    page = requests.get('https://www.basketball-reference.com/players/b/bogutan01.html')
    soup = BeautifulSoup(page.text, 'html.parser')
    per_game_stats = soup.find(id='per_game.2019')
    flat_list = [item for sublist in per_game_stats for item in sublist]
    games_played = flat_list[5]
    games_started = flat_list[6]
    avg_mins = flat_list[7]

    data = {}
    data['games_played'] = games_played
    data['games_started'] = games_started
    data['avg_mins'] = avg_mins
    #json_data = json.dumps(data)

    # Write
    with open(file_name, 'w') as outfile:
        json.dump(data, outfile)

    print(f"Wrote out gzipped JSON file to {file_name}")

    # Upload
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('bogut-minutes')
    bucket.upload_file(
        file_name,
        file_name,
        ExtraArgs={
            'ACL':'public-read',
#            'ContentEncoding': 'gzip',
            'ContentType': "application/json"
        }
    )
    print("Uploaded tweets.json to S3 bucket")


if __name__ == '__main__':
    main()
