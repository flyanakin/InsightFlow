import pandas as pd
import pytest
from dagster import build_asset_context
from dotenv import load_dotenv
import os

from quickstart_etl.resources import EnvResource
from quickstart_etl.assets.message import portfolio_analysis_sheet, wework_bot_message

load_dotenv()
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
WECHAT_TOKEN = os.getenv('WECOM_BOT_TOKEN_KIKYO')

test_env_resource = EnvResource(tushare_token=TUSHARE_TOKEN,
                                airtable_api_token=AIRTABLE_API_TOKEN,
                                airtable_base_id=AIRTABLE_BASE_ID,
                                wechat_token=WECHAT_TOKEN)


@pytest.fixture
def online_portfolio():
    df = pd.read_csv('online_portfolio_test_data.csv')
    return df

def test_portfolio_analysis_sheet(online_portfolio):
    context = build_asset_context()
    result = portfolio_analysis_sheet(context,test_env_resource,online_portfolio)
    print(result)
    assert result.empty == True