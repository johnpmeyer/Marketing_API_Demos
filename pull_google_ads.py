import os

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

import pandas as pd

from sqlalchemy import create_engine, exc

from utils.auth import ps_connect
from utils.db_utils import load_updates

googleads_client = GoogleAdsClient.load_from_storage(version="v10")

ga_service = googleads_client.get_service("GoogleAdsService")

query = """
    SELECT
      segments.date,
      campaign.id,
      campaign.name,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      metrics.all_conversions,
      metrics.all_conversions_value
    FROM campaign WHERE segments.date DURING LAST_7_DAYS
    AND metrics.impressions > 0
    ORDER BY metrics.cost_micros DESC"""

search_request = googleads_client.get_type("SearchGoogleAdsStreamRequest")
search_request.customer_id = os.environ.get("GOOGLE_ADS_MCC_ID").replace("-", "")

app_data_google_combined = pd.DataFrame(
        columns=['date', 'campaign_id', 'campaign_name', 'advertising_channel', 'impressions', 'clicks',
                 'cost', 'all_conversions', 'all_conversions_value'])

search_request.query = query
stream = ga_service.search_stream(search_request)
for batch in stream:
    for row in batch.results:
        date = row.segments.date
        campaign_id = row.campaign.id
        campaign_name = row.campaign.name
        impressions = row.metrics.impressions
        clicks = row.metrics.clicks
        cost = row.metrics.cost_micros / 1000000
        all_conversions = row.metrics.all_conversions
        all_conversions_value = row.metrics.all_conversions_value
        print(
            f'Campaign "{campaign_name}" '
            f"with ID {campaign_id} "
            f"had {impressions} impression(s), "
            f"{clicks} click(s), and "
            f"{cost} cost during "
            "the last 7 days."
        )

        app_data_google_combined = app_data_google_combined.append(
            {'date': date,
             'campaign_id': str(campaign_id),
             'campaign_name': str(campaign_name),
             'advertising_channel': 'Search',
             'impressions': int(impressions),
             'clicks': int(clicks),
             'cost': float(cost),
             'all_conversions': all_conversions,
             'all_conversions_value': all_conversions_value,
             },
            ignore_index=True)


engine = ps_connect(os.environ.get("marketing_db_name")) #connect to database. Defined in utils folder.

print("Uploading data to table")

#Process here is defined in utils function for db_utils
try:
    load_updates(app_data_google_combined, 'google_ad_day', engine, 'google_ad_day_un')
except exc.IntegrityError as err:
    print(err.orig.args[0])