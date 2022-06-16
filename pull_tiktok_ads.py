# coding=utf-8
import json
import requests
import os
import pandas as pd

from datetime import date, timedelta, datetime

from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse

from utils.auth import ps_connect
from utils.db_utils import load_updates

from sqlalchemy import create_engine, exc

ACCESS_TOKEN = os.environ.get("tiktok_access_token")
PATH = "/open_api/v1.2/reports/integrated/get/"

START_DATE = (date.today() - timedelta(days=3)).strftime("%Y-%m-%d")
END_DATE = (date.today()).strftime("%Y-%m-%d")

def build_url(path, query=""):
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get(json_str):
    # type: (str) -> dict
    """
    Send GET request
    :param json_str: Args in JSON format
    :return: Response in JSON format
    """
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": ACCESS_TOKEN,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()

def process_df(json_blob, df_name = "raw_tiktok_ads_cpg"):
    """
    Create Dataframe using JSON object from get() function
    :param json_blob: JSON object from get()
    :param df_name: Name of dataframe.
    :return: Pandas Dataframe
    """

    print(json_data)

    final_df = pd.DataFrame(columns=['date', 'campaign_id', 'campaign_name', 'advertising_channel',
                                     'impressions', 'clicks', 'cost'])

    for i in range(0, len(json_data['data']['list'])):
        date = json_data['data']['list'][i]['dimensions']['stat_time_day']
        campaign_id = json_data['data']['list'][i]['dimensions']['campaign_id']
        campaign_name = json_data['data']['list'][i]['metrics']['campaign_name']
        advertising_channel = 'tiktok'
        impressions = json_data['data']['list'][i]['metrics']['impressions']
        clicks = json_data['data']['list'][i]['metrics']['clicks']
        cost = json_data['data']['list'][i]['metrics']['spend']

        final_df = final_df.append(
            {'date': date,
             'campaign_id': campaign_id,
             'campaign_name': campaign_name,
             'advertising_channel': advertising_channel,
             'impressions': impressions,
             'clicks': clicks,
             'cost': cost
             },
            ignore_index=True)

    return final_df


if __name__ == '__main__':
    metrics_list = ["campaign_name", "spend", "impressions", "clicks"]
    metrics = json.dumps(metrics_list)
    data_level = "AUCTION_CAMPAIGN"
    end_date = END_DATE
    #order_type = ORDER_TYPE
    #order_field = ORDER_FIELD
    page_size = 1000
    start_date = START_DATE
    advertiser_id = os.environ.get("tiktok_ad_id")
    #filter_value = FILTER_VALUE
    #field_name = FIELD_NAME
    #filter_type = FILTER_TYPE
    service_type = "AUCTION"
    lifetime = False
    report_type = "BASIC"
    page = 1
    dimensions_list = ["campaign_id", "stat_time_day"]
    dimensions = json.dumps(dimensions_list)

    # Args in JSON format
    my_args = "{\"metrics\": %s, \"data_level\": \"%s\", \"end_date\": \"%s\", \"page_size\": \"%s\", " \
              "\"start_date\": \"%s\", \"advertiser_id\": \"%s\", \"service_type\": \"%s\", \"lifetime\": \"%s\", " \
              "\"report_type\": \"%s\", \"page\": \"%s\", \"dimensions\": %s}" % \
              (metrics, data_level, end_date, page_size, start_date, advertiser_id,
               service_type, lifetime, report_type, page, dimensions)

    json_data = get(my_args)

    df = process_df(json_data)

    engine = ps_connect(os.environ.get("marketing_db_name")) #connect to database. Defined in utils folder.

    print("Uploading data to table")

    #Process here is defined in utils function for db_utils
    try:
        load_updates(df, 'tiktok_ad_day', engine, 'tiktok_ads_un')
    except exc.IntegrityError as err:
        print(err.orig.args[0])

    print("Done")

