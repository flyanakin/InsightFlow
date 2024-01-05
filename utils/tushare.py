import pandas as pd
from datetime import datetime,time,date,timedelta
import math
import tushare as ts
import os
tushare_token = os.getenv('TUSHARE_TOKEN')


def get_current_price() -> pd.DataFrame:
    """
    获取当前股票价格
    :return:
    """
    ts.set_token(tushare_token)
    pro = ts.pro_api()
    # 获取交易日历
    cal_df = pro.trade_cal(exchange="", start_date="20210101", end_date="20301231")

    # 获取当前日期和时间
    current_datetime = datetime.now()

    # 如果当前时间早于收盘时间（例如，15:30），则将最近交易日设置为前一天
    if current_datetime.time() < time(15, 30):
        current_date = (current_datetime - pd.Timedelta(days=1)).strftime("%Y%m%d")
    else:
        current_date = current_datetime.strftime("%Y%m%d")
    print(f'current_date:\n{current_date}')


    # 找到最近的交易日
    # 找到不晚于当前日期的最近的交易日
    latest_trade_date = (
        cal_df[cal_df["cal_date"] <= current_date]["cal_date"]
        .sort_values(ascending=False)
        .tolist()[0]
    )
    print(f'latest_trade_date:\n{latest_trade_date}')
    latest_stock_df = pro.daily(
        trade_date=latest_trade_date, fields="ts_code,close"
    )
    latest_fund_df = pro.fund_daily(
        trade_date=latest_trade_date, fields="ts_code,close"
    )
    latest_price_df = pd.concat([latest_stock_df, latest_fund_df])
    return latest_price_df

def moving_sell_strategy(stock: pd.Series):
    # todo: pd.series这种传参方式需要改
    ts.set_token(tushare_token)
    pro = ts.pro_api()
    ##取最新数据，这里要取最新是因为排除复权问题
    today = date.strftime(date.today(), "%Y%m%d")
    df = pro.query('daily', ts_code=stock['代码'], start_date='20220101', end_date=today)
    data = df.set_index("trade_date")
    max = data['close'].max()

    profit_ratio = (stock["最新价格"] - stock['成本价']) / stock['成本价']
    max_ratio = (max - stock['成本价']) / stock['成本价']
    ##计算处于哪个阶梯，25%为一段，下跌8%止盈
    stage = int(math.log((1 + max_ratio), 1.25))
    if stock["最新价格"] < 0.92 * (math.pow(1.25, stage)):
        signal = 'sell'
    else:
        signal = 'hold'

    return stage, signal


def turnover_sell_strategy(stock: pd.Series):
    # todo: pd.series这种传参方式需要改

    ts.set_token(tushare_token)
    pro = ts.pro_api()

    today = date.strftime(date.today(), "%Y%m%d")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")
    df = pro.query(
        'daily_basic', ts_code=stock['代码'], start_date=start_date, end_date=today
    )
    df.sort_values(by=['trade_date'], inplace=True, ascending=False)
    total_mv = df.loc[0, 'total_mv'] / 10000
    turnover_rate = df.loc[0, 'turnover_rate']

    if total_mv <= 50 and turnover_rate > 35:
        signal = 'sell'
    elif 50 < total_mv <= 100 and turnover_rate > 25:
        signal = 'sell'
    elif 100 < total_mv <= 200 and turnover_rate > 15:
        signal = 'sell'
    elif 200 < total_mv <= 400 and turnover_rate > 10:
        signal = 'sell'
    elif total_mv > 400 and turnover_rate > 7:
        signal = 'sell'
    else:
        signal = 'hold'

    return signal, round(total_mv), turnover_rate