from dagster import asset, AssetExecutionContext, AssetIn
from utils.airtable import Airtable
from utils.tushare import get_last_trade_date, moving_sell_strategy, turnover_sell_strategy
from quickstart_etl.resources import EnvResource
import requests
import json
import pandas as pd
import tushare as ts


@asset()
def online_portfolio(context: AssetExecutionContext, env: EnvResource) -> pd.DataFrame:
    airtable = Airtable(
        api_key=env.airtable_api_token,
        table_name="portfolio online",
        base_id=env.airtable_base_id,
    )
    records = airtable.get_all_records()
    df = pd.DataFrame.from_records([record["fields"] for record in records])
    context.log.info(f"rows:\n{len(df)}")
    context.log.info(f"online_portfolio:\n{df}")
    return df


@asset(ins={"online_portfolio": AssetIn(key=["online_portfolio"])})
def portfolio_analysis_sheet(context: AssetExecutionContext, env: EnvResource, online_portfolio: pd.DataFrame) -> pd.DataFrame:
    ts.set_token(env.tushare_token)
    pro = ts.pro_api()
    calendar = pro.trade_cal(exchange='', start_date='20210101', end_date='20301231')
    calendar = calendar[calendar['is_open'] == 1]
    last_trade_date = get_last_trade_date(calendar)
    latest_stock_df = pro.daily(
        trade_date=last_trade_date, fields="ts_code,close"
    )
    latest_fund_df = pro.fund_daily(
        trade_date=last_trade_date, fields="ts_code,close"
    )
    latest_price_df = pd.concat([latest_stock_df, latest_fund_df])
    latest_price_df = latest_price_df.rename(columns={'ts_code': '代码', 'close': '最新价格'})

    df = pd.merge(online_portfolio, latest_price_df, on='代码', how='left')
    df.loc[df['代码'] != '999999.ZZ', '当前价'] = df['最新价格']
    df['总成本'] = df['成本价'] * df['持仓数量']
    print(df[['资产名称','当前价']])

    pivoted_to_p = df.groupby("组合")[['总成本', '最新市值', '持仓盈亏']].sum()
    total_mv = pivoted_to_p['最新市值'].sum()
    pivoted_to_p['仓位'] = pivoted_to_p['最新市值'] / total_mv
    pivoted_to_p['组合盈亏比例'] = pivoted_to_p['持仓盈亏'] / pivoted_to_p['总成本']
    return pivoted_to_p


@asset(ins={"portfolio_analysis_sheet": AssetIn(key=["portfolio_analysis_sheet"])})
def wework_bot_message(context: AssetExecutionContext, env: EnvResource, portfolio_analysis_sheet: pd.DataFrame):
    message = ''
    hook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={env.wechat_token}"
    post_data = {"msgtype": "text", "text": {"content": message}}

    headers = {"Content-Type": "application/json"}
    requests.post(
        url=hook_url,
        headers=headers,
        data=json.dumps(post_data),
    )