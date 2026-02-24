"""直接读取数据库元数据进行核对"""
import sys
from pathlib import Path

# 项目根目录
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

# 数据库路径
db_path = Path(r"E:\projects\qrp-atlas\data\db\quant.db")

print("=" * 80)
print("QRP Atlas 数据库元数据核对报告（直接读取数据库）")
print("=" * 80)

print(f"\n数据库路径: {db_path}")
print(f"文件存在: {db_path.exists()}")

if not db_path.exists():
    print("\n❌ 数据库文件不存在！")
    sys.exit(1)

print(f"文件大小: {db_path.stat().st_size / 1024:.2f} KB")

try:
    import duckdb
    import pandas as pd

    print("\n" + "=" * 80)
    print("连接数据库...")
    con = duckdb.connect(str(db_path), read_only=True)
    print("✅ 连接成功")

    print("\n" + "=" * 80)
    print("1. 数据库表清单")
    print("=" * 80)

    tables = con.execute("SHOW TABLES").fetchall()
    actual_table_names = [t[0] for t in tables]
    print(f"\n实际表名: {actual_table_names}")

    from qrp_atlas.contracts import ALL_TABLES
    expected_table_names = [t.name for t in ALL_TABLES]
    print(f"预期表名 (contracts): {expected_table_names}")

    missing_tables = set(expected_table_names) - set(actual_table_names)
    extra_tables = set(actual_table_names) - set(expected_table_names)

    if missing_tables:
        print(f"\n❌ 缺失表: {missing_tables}")
    if extra_tables:
        print(f"\n⚠️  多余表: {extra_tables}")
    if not missing_tables and not extra_tables:
        print("\n✅ 表清单与 contracts 完全一致")

    print("\n" + "=" * 80)
    print("2. 各表结构详细比对（数据库实际 vs contracts 预期）")
    print("=" * 80)

    from qrp_atlas.contracts import TABLE_BY_NAME

    for table_name in actual_table_names:
        print(f"\n{'=' * 80}")
        print(f"表名: {table_name}")
        print(f"{'=' * 80}")

        # 获取实际表结构
        df_actual = con.execute(f"DESCRIBE {table_name}").fetchdf()
        print("\n【数据库实际结构】")
        print(df_actual.to_string(index=False))

        # 获取建表 SQL
        create_sql = con.execute(f"""
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name='{table_name}'
        """).fetchone()
        if create_sql:
            print("\n【数据库建表语句】")
            print(create_sql[0])

        # 获取预期结构
        if table_name in TABLE_BY_NAME:
            expected = TABLE_BY_NAME[table_name]
            print(f"\n【contracts 预期结构】")
            for col in expected.columns:
                null_str = "NOT NULL" if not col.nullable else ""
                print(f"  {col.name:20} {col.dtype:15} {null_str}")
            print(f"  主键: {expected.primary_key}")

            # 字段比对
            actual_cols = set(df_actual["column_name"].tolist())
            expected_cols = set(expected.column_names())

            print(f"\n【字段比对】")
            missing_cols = expected_cols - actual_cols
            extra_cols = actual_cols - expected_cols

            if missing_cols:
                print(f"❌ 缺失字段: {missing_cols}")
            if extra_cols:
                print(f"⚠️  多余字段: {extra_cols}")
            if not missing_cols and not extra_cols:
                print("✅ 字段列表一致")

            print(f"\n【字段类型比对】")
            all_match = True
            for _, row in df_actual.iterrows():
                col_name = row["column_name"]
                actual_type = row["column_type"]
                if col_name in expected_cols:
                    exp_col = next(c for c in expected.columns if c.name == col_name)
                    # 简化类型比较（忽略大小写和长度修饰）
                    actual_type_simple = actual_type.upper().split("(")[0]
                    expected_type_simple = exp_col.dtype.upper().split("(")[0]
                    match = actual_type_simple == expected_type_simple
                    status = "✅" if match else "❌"
                    if not match:
                        all_match = False
                    print(f"  {status} {col_name:20} 实际: {actual_type:15} 预期: {exp_col.dtype:15}")

            if all_match:
                print("\n✅ 所有字段类型一致")

            # 主键比对
            print(f"\n【主键比对】")
            if "PRIMARY KEY" in create_sql[0]:
                print("✅ 数据库有主键")
            print(f"预期主键: {expected.primary_key}")

        # 行数统计
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"\n【数据行数】: {row_count}")

        # 预览数据
        if row_count > 0:
            print("\n【前 3 行数据预览】")
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
            print(sample.to_string(index=False))

    con.close()

except Exception as e:
    print(f"\n❌ 错误: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("核对完成")
print("=" * 80)
