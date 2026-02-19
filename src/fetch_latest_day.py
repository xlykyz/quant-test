import akshare as ak
import pandas as pd
from datetime import datetime, time
from zoneinfo import ZoneInfo
from pathlib import Path


# ==============================
# 配置：中国时区
# ==============================

CHINA_TZ = ZoneInfo("Asia/Shanghai")


# ==============================
# 获取最近有效交易日
# ==============================

def get_latest_trade_date():

    now = datetime.now(CHINA_TZ)

    # 获取A股交易日历
    trade_dates = ak.tool_trade_date_hist_sina()

    trade_dates["trade_date"] = pd.to_datetime(
        trade_dates["trade_date"]
    )

    today = pd.Timestamp(now.date())

    # 情况1：今天不是交易日
    if today not in trade_dates["trade_date"].values:

        latest = trade_dates[trade_dates["trade_date"] < today].iloc[-1]["trade_date"]

        return latest


    # 情况2：今天是交易日但未收盘
    if now.time() < time(15, 1):

        latest = trade_dates[
            trade_dates["trade_date"] < today
        ].iloc[-1]["trade_date"]

        return latest


    # 情况3：今天已收盘
    return today


# ==============================
# 构建文件路径
# data/daily/年份/YYYY-MM-DD_Astock.csv
# ==============================

def build_file_path(trade_date):

    year = trade_date.year

    base_dir = Path("data") / "daily" / str(year)

    base_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{trade_date.date()}_Astock.csv"

    return base_dir / file_name


# ==============================
# 获取并保存数据
# ==============================

def fetch_and_save(trade_date):

    file_path = build_file_path(trade_date)

    # 避免重复下载
    if file_path.exists():

        print(f"文件已存在，跳过下载: {file_path}")

        return


    print(f"正在获取 A股数据: {trade_date.date()}")


    # 获取全市场实时快照（收盘后即为日线）
    df = ak.stock_zh_a_spot_em()


    # 添加交易日字段
    df.insert(0, "trade_date", trade_date.date())


    # 保存
    df.to_csv(
        file_path,
        index=False,
        encoding="utf-8-sig"
    )


    print(f"保存成功: {file_path}")

    print(f"股票数量: {len(df)}")


# ==============================
# 主程序入口
# ==============================

def main():

    trade_date = get_latest_trade_date()

    print(f"当前中国时间: {datetime.now(CHINA_TZ)}")

    print(f"目标交易日: {trade_date.date()}")

    fetch_and_save(trade_date)


# ==============================

if __name__ == "__main__":

    main()
