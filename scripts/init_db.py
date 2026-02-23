from pathlib import Path
import duckdb

# 获取项目根目录（src 的上一级）
BASE_DIR = Path(__file__).resolve().parent.parent

# 数据库目录
DB_DIR = BASE_DIR / "data" / "db"

# 确保目录存在
DB_DIR.mkdir(parents=True, exist_ok=True)

# 数据库文件路径
DB_PATH = DB_DIR / "quant.db"

# 连接数据库
con = duckdb.connect(str(DB_PATH))

con.execute("BEGIN TRANSACTION;")
# 创建daily_market_snapshot 用途：每日全市场行情增量保存
con.execute("""
CREATE TABLE IF NOT EXISTS daily_market_snapshot (
  trade_date DATE,
  ticker VARCHAR,
  name VARCHAR,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  pct_change DOUBLE,
  volume BIGINT,
  amount DOUBLE,
  turnover DOUBLE,
  market_cap DOUBLE,
  float_cap DOUBLE,
  is_st BOOLEAN,
  is_limit_up BOOLEAN,
  is_limit_down BOOLEAN,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date, ticker)
  );
""")

# 创建market_phase 用途：每日判断层阶段记录
con.execute("""
  CREATE TABLE IF NOT EXISTS market_phase (
  trade_date DATE PRIMARY KEY,
  phase VARCHAR,
  M1_core BOOLEAN,
  M2_front BOOLEAN,
  M3_identifiable BOOLEAN,
  V_triggered BOOLEAN,
  notes VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
""")

# 创建trade_execution 用途：记录交易执行全过程
con.execute("""
  CREATE TABLE IF NOT EXISTS trade_execution (
  trade_id VARCHAR PRIMARY KEY,
  ticker VARCHAR,
  entry_date DATE,
  entry_price DOUBLE,
  path_type VARCHAR,
  half_sell_trigger DOUBLE,
  half_sell_date DATE,
  half_sell_price DOUBLE,
  exit_date DATE,
  exit_price DOUBLE,
  position_pct DOUBLE,
  notes VARCHAR
  );
""")
con.execute("COMMIT;")

print(f"数据库初始化完成: {DB_PATH}")

tables = con.execute("SHOW TABLES").fetchall()

print(f"数据库路径: {DB_PATH}")
print("已创建表:")

for table in tables:
    print(" -", table[0])

con.close()
