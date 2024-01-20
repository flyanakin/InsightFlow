from dagster import asset, AssetExecutionContext, AssetIn
from utils.airtable import Airtable
from utils.tushare import get_current_price, moving_sell_strategy, turnover_sell_strategy
from utils.webhook import send_wework_message
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

    latest_price_df = get_current_price(context)
    context.log.debug(f"latest_price_df:\n{latest_price_df}")
    latest_price_df = latest_price_df.rename(columns={'ts_code': '代码', 'close': '最新价格'})
    df = pd.merge(df, latest_price_df, on='代码', how='left')
    df['持仓盈亏'] = (df['最新价格'] - df['成本价']) * df['持仓数量']
    df['持仓盈亏比例'] = df['持仓盈亏'] / (df['成本价'] * df['持仓数量'])

    context.log.info(f"rows:\n{len(df)}")
    context.log.info(f"online_portfolio:\n{df}")
    return df


@asset(ins={"online_portfolio": AssetIn(key=["online_portfolio"])})
def sub_portfolio_sheet(context: AssetExecutionContext, online_portfolio: pd.DataFrame):

    online_portfolio['总成本'] = online_portfolio['成本价'] * online_portfolio['持仓数量']

    pivoted = online_portfolio.groupby("组合名称")[['总成本', '最新市值', '持仓盈亏']].sum()
    total_mv = pivoted['最新市值'].sum()
    pivoted['仓位'] = pivoted['最新市值'] / total_mv
    total_position = pivoted.loc[pivoted.index != '现金', '仓位'].sum()
    pivoted['组合盈亏比例'] = pivoted['持仓盈亏'] / pivoted['总成本']
    portfolio_profit_str = ""
    for index, row in pivoted.iterrows():
        if index != '其他' and index != '现金':
            profit_str = f"· {index}组合：持仓收益{round(row['组合盈亏比例'] * 100, 2)}%，仓位：{round(row['仓位'] * 100, 2)}%"
            portfolio_profit_str = portfolio_profit_str + profit_str + '\n'

    ##组合剪枝计算
    cut_l = []
    for index, row in online_portfolio.iterrows():
        if row['组合名称'] != '其他':
            if row['持仓盈亏比例'] < -0.1:
                stock_cut_str = f"{row['名称']}({row['组合名称']},{round(row['持仓盈亏比例']*100, 2)})"
                cut_l.append(stock_cut_str)
    stock_cut_l = "、".join(cut_l) if cut_l else '无'

    message = f'''
组合概览表现：
· 总仓位：{round((total_position * 100),2)}%
{portfolio_profit_str}

止损提醒：
· 组合剪枝：{stock_cut_l}
    '''
    context.log.info(pivoted)
    context.log.info(message)
    send_wework_message(message)


@asset(ins={"online_portfolio": AssetIn(key=["online_portfolio"])})
def profit_loss_sheet(context: AssetExecutionContext, online_portfolio: pd.DataFrame):
    ##止盈计算
    sell_result = ""
    turn_result = ""
    for index, row in online_portfolio.iterrows():
        if (
                row['持仓盈亏比例'] > 0.1
                and row['组合名称'] != '其他'
                and row['类型'] == '股票'
        ):
            profit = round(row['持仓盈亏比例'] * 100, 2)
            stage, signal = moving_sell_strategy(row)
            context.log.info(f"{row['名称']} {stage} {signal}")
            sell_str = f"{row['名称']}(阶梯{stage}，持仓收益{profit}%，{signal})"
            sell_result = sell_result + sell_str + "\n"
            if stage > 0:
                turnover_signal, total_mv, turnover_rate = turnover_sell_strategy(row)
                turn_str = f"{row['名称']}(持仓收益{profit}%，市值{total_mv}亿,今日换手率{turnover_rate}%，{turnover_signal})"
                context.log.info(f"{total_mv},{turnover_rate}")
                turn_result = turn_result + turn_str + "\n"

    message = f'''
止盈提醒：
· 25/8移动止盈：
{sell_result}
· 换手率止盈：
{turn_result}
    '''
    context.log.info(message)
    send_wework_message(message)