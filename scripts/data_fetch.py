import akshare as ak
import pandas as pd
from pathlib import Path

# 获取平安银行日线数据
df = ak.stock_zh_a_hist(
    symbol="000001",
    period="daily",
    start_date="20240101",
    end_date="20251231",
    adjust="qfq"
)

# 保存到本地
data_dir = Path("data")
file_path = data_dir / "000001.csv"
df.to_csv(file_path, index=False, encoding="utf-8-sig")

print("数据获取成功")
print(df.tail())
