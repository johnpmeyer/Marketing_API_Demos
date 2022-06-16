import argparse

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi

import os

from datetime import date, timedelta, datetime

import pandas as pd #for dataframes

from sqlalchemy import create_engine, exc

from utils.auth import ps_connect
from utils.db_utils import load_updates

#Main action happens here. We pull one day at a time and store each campaign data as a row in a given day's dataframe.
#This in turns gets concatted into a larger multi-day table that is 'upserted' (past values replaced with updated data)
#and new data is simply added to the table.
def pull_one_day_app_data(date_input, account):
    print("On Day: "+date_input)

    # Creating time string which allows custom date pulls. One day in this case
    time_range_string = ("{'since':'" +
                         str(date_input) +
                         "', 'until':'" +
                         str(date_input) +
                         "'}")

    #Defines the limit of campaigns to pull, timeframe, and level (campaign) even though we're pulling from account obj.
    params = {'limit': '250',
              'time_range': time_range_string,
              'level': 'campaign'
              }

    # What we want to pull from Facebook.
    fields = [
        'campaign_id',
        'campaign_name',
        'impressions',
        'clicks',
        'outbound_clicks',
        'inline_link_clicks',
        'spend',
        'actions',
        'action_values'
    ]

    #Empty dataframe for a single date of data.
    app_data_facebook_df_one_day = \
        pd.DataFrame(columns=['date', 'campaign_id', 'campaign_name', 'impressions', 'all_clicks',
                              'cost', 'all_conversions', 'all_conversions_value', 'outbound_clicks'])

    #Pulls campaign data into dictionary-like object. Each object has approximate structure like:
    #{Campaign: , Clicks: , etc.}
    #{Also nested dictionary for each conversion, so {Actions: {Registrations: {}}
    campaign_data = account.get_insights(fields=fields, params=params) ##pull all campaign data for date-range

    #loop through each campaign in campaign_data
    for item in campaign_data:
        #print("On item: " + item['campaign_name'])

        #Initially set conversion and link click values to zero. No entry will appear for these in dict if empty,
        #so a try-catch is necessary to avoid Key Errors where the key for 'actions' doesn't exist.

        all_conversions = 0
        all_conversions_value = 0
        inline_link_clicks = 0

        try:  # combine all actions into one number for main table. start with actions
            actions = item["actions"]  # throws key error for no 'actions' key.

            for action in actions:  # actions list is a list of dictionaries. iterate through and add all values.
                all_conversions += int(action['value'])

        except (IndexError, KeyError):  #if there's no key, no actions have occured. all_conversions is zero from above.
            pass

        try:  # similar to immediately above, combine action_values (monetary) into one table.
            action_values = item["action_values"]

            for action in action_values:  # actions list is a list of dictionaries. iterate through and add all values.
                all_conversions_value += float(action['value'])

        except (IndexError, KeyError):
            pass

        try:  # avoid errors for inline link clicks not having entry.
            inline_link_clicks = int(item["inline_link_clicks"])

        except (IndexError, KeyError):
            pass

        #take all data above from dictionary-like object and append to 'one-day' campaign.
        app_data_facebook_df_one_day = app_data_facebook_df_one_day.append(
            {'date': item["date_start"],
             'campaign_id': str(item["campaign_id"]),
             'campaign_name': str(item["campaign_name"]),
             'impressions': int(item["impressions"]),
             'all_clicks': int(item["clicks"]),
             'cost': float(item["spend"]),
             'all_conversions': all_conversions,
             'all_conversions_value': all_conversions_value,
             'outbound_clicks': inline_link_clicks
             },
            ignore_index=True)

    #after loop, return one-day table that has each campaign by day and its performance.
    return app_data_facebook_df_one_day

#Main Function that gets arguments from parser in other block below.
def main (start_date, end_date, account):
    # Create empty pandas DF that will ultimately upload to Postgres.
    app_data_facebook_combined = pd.DataFrame(
        columns=['date', 'campaign_id', 'campaign_name', 'impressions', 'all_clicks',
                 'cost', 'all_conversions', 'all_conversions_value', 'outbound_clicks'])

    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    # Control structure to iterate through each day and combine daily campaign data into one table.
    # I.E. data from 7/21-7/24, 2021 segmented by campaign would be here.
    while start_date != end_date:
        one_day_data = pull_one_day_app_data(str(start_date), account) #pull one day of data - entire function above.
        app_data_facebook_combined = app_data_facebook_combined.append(one_day_data) #append day's DF to final DF.
        start_date = start_date + timedelta(days=1) #Increment by one day. Loop stops when start_date = end_date

    #append one final day
    one_day_data = pull_one_day_app_data(str(start_date), account)
    app_data_facebook_combined = app_data_facebook_combined.append(one_day_data)

    app_data_facebook_combined['advertising_channel'] = 'Social' #Field in database that needs to be added.

    engine = ps_connect(os.environ.get("marketing_db_name")) #connect to database. Defined in utils folder.

    print("Uploading data to table")

    #Process here is defined in utils function for db_utils
    try:
        load_updates(app_data_facebook_combined, 'facebook_ad_day', engine, 'facebook_ad_day_un')
    except exc.IntegrityError as err:
        print(err.orig.args[0])

    print("Done")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start_date", default=(date.today() - timedelta(days=3)).strftime("%Y-%m-%d"),
                        help="Beginning report date in format YYYY-MM-DD. Default is three days ago (inclusive).")
    parser.add_argument("-e", "--end_date", default=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
                        help="Ending report date in format YYYY-MM-DD. Default is yesterday (inclusive).")
    args = vars(parser.parse_args())

    # Getting IDs relevant to initializing Facebook Ads API
    my_app_id = os.environ.get("fb_app_id")
    my_app_secret = os.environ.get("fb_client_secret")
    my_access_token = os.environ.get("fb_access_token")
    niche_user_acct = os.environ.get("fb_user_acct")
    #niche_user_acct = os.environ.get("fb_niche_acct")
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token, api_version='v12.0')  # initializes api

    # Getting account data from API.
    account = AdAccount('act_' + niche_user_acct)

    main(args['start_date'], args['end_date'], account)