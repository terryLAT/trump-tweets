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
    Downloads all Bogut stats we want from...
    https://www.basketball-reference.com/players/b/bogutan01/gamelog/2019/  
    Uploads them as a single JSON to S3.
    
    Currently deployed to Heroku on a scheduler, runnining hourly.
    """
    # Collect and parse page
    page = requests.get('https://www.basketball-reference.com/players/b/bogutan01.html')
    soup = BeautifulSoup(page.text, 'html.parser')
    per_game_stats = soup.find(id='per_game.2019')
    flat_list = [item for sublist in per_game_stats for item in sublist]
    games_played = flat_list[5]
    games_started = flat_list[6]
    avg_mins = flat_list[7]

    #adding more data from second page
    page_two = requests.get('https://www.basketball-reference.com/players/b/bogutan01/gamelog/2019/')
    soup_two = BeautifulSoup(page_two.text, 'html.parser')
    detailed_stats = soup_two.find(id='pgl_basic')
    flat_list_two = [item for sublist in detailed_stats for item in sublist]

    #remove column headers and newlines
    cleaned = flat_list_two[38:]
    even_cleaner = cleaned[0::2]
    spec_iter = 0
    for item in even_cleaner:
        even_cleaner[spec_iter] = item.get_text(separator=',')
        spec_iter += 1

    data = {}
    data['games_played'] = games_played
    data['games_started'] = games_started
    data['avg_mins'] = avg_mins
    data['game_data'] = even_cleaner

    json_data = json.dumps(data)

    #Write
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
