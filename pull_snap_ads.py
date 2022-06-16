from datetime import date, timedelta

from utils.auth import ps_connect
from utils.db_utils import load_updates

from sqlalchemy import create_engine, exc

import requests
import os
import pandas as pd

#refresh access token
url = 'https://accounts.snapchat.com/login/oauth2/access_token'

dict = {'client_id': os.environ.get("snap_client_id"),
        'client_secret': os.environ.get("snap_client_secret"),
        'grant_type': 'refresh_token',
        'refresh_token': os.environ.get("snap_refresh_token")
        }

refresh_data = requests.post(url, dict)

#get ad account data
url = os.environ.get("snap_ad_url")

auth_name = 'Bearer ' + refresh_data.json()['access_token']
hdr = {'Authorization': auth_name}

req = requests.get(url, headers = hdr)

cpg_list = req.json()['campaigns']
cpg_list = [item['campaign']['id'] for item in cpg_list]

start_date = date.today() - timedelta(days=2)
end_date = date.today()

def pull_one_date_data(date):
        final_df = pd.DataFrame(columns=['date', 'campaign_id', 'campaign_name', 'advertising_channel',
                                         'impressions', 'clicks', 'cost'])

        start_date_str = str(date) + 'T00:00:00.000-04:00'
        end_date_str = str(date + timedelta(days=1)) + 'T00:00:00.000-04:00'

        print(start_date_str, end_date_str)

        for i, item in enumerate(cpg_list):
                cpg_name = req.json()['campaigns'][i]['campaign']['name']
                cpg_id = item

                url = 'https://adsapi.snapchat.com/v1/campaigns/' + cpg_id + '/stats/'
                dict = {"granularity": "DAY",
                        "start_time": start_date_str,
                        "end_time": end_date_str,
                        "fields": "paid_impressions,swipes,spend"}


                req_internal = requests.get(url, dict, headers = hdr)
                data = req_internal.json()

                try:
                    impressions = data['timeseries_stats'][0]['timeseries_stat']['timeseries'][0]['stats']['paid_impressions']
                    swipes = data['timeseries_stats'][0]['timeseries_stat']['timeseries'][0]['stats']['swipes']
                    spend = float(data['timeseries_stats'][0]['timeseries_stat']['timeseries'][0]['stats']['spend'] / 1000000)

                    final_df = final_df.append(
                            {'date': date,
                             'campaign_id': cpg_id,
                             'campaign_name': cpg_name,
                             'advertising_channel': 'Snapchat Ads',
                             'impressions': impressions,
                             'clicks': swipes,
                             'cost': spend
                             },
                            ignore_index=True)
                except:
                    print(data)

        return final_df

app_data_snap_combined = pd.DataFrame(columns=['date', 'campaign_id', 'campaign_name', 'advertising_channel',
                                         'impressions', 'clicks', 'cost'])

while start_date != end_date:
        print("On Date:",str(start_date))
        one_day_data = pull_one_date_data(start_date)  # pull one day of data - entire function above.
        app_data_snap_combined = app_data_snap_combined.append(one_day_data)  # append day's DF to final DF.
        start_date = start_date + timedelta(days=1)  # Increment by one day. Loop stops when start_date = end_date


engine = ps_connect(os.environ.get("marketing_db_name")) #connect to database. Defined in utils folder.

print("Uploading data to table")

#Process here is defined in utils function for db_utils
try:
        load_updates(app_data_snap_combined, 'snap_ad_day', engine, 'snap_ad_day_un')
except exc.IntegrityError as err:
        print(err.orig.args[0])


