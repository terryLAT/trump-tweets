import requests
import datetime


def main():
    base_url = 'http://www.trumptwitterarchive.com/data/realdonaldtrump/{}.json'
    years = range(2009, datetime.date.today().year)
    json_list = [requests.get(base_url.format(year)).json() for year in years]
    flat_list = [item for sublist in json_list for item in sublist]
    print(f"{len(flat_list)} tweets found")


if __name__ == '__main__':
    main()
