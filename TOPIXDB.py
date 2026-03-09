import sqlite3
import os
import glob
import pandas as pd

# データベース保存先
db_dir = r'H:\INDEX\TOPIX\資料\浮動株比率過去データ\TOPIXデータベース'
db_path = os.path.join(db_dir, 'topix_index.db')
os.makedirs(db_dir, exist_ok=True)

# CSVファイル保存先
master_csv_dir = r'H:\INDEX\TOPIX\資料\浮動株比率過去データ\TOPIXデータ'
change_csv_dir = r'H:\INDEX\TOPIX\資料\浮動株比率過去データ\変更予告TOPIX'

# DB接続・テーブル作成
conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS master_of_index (
    日付 TEXT,
    銘柄コード TEXT,
    銘柄名 TEXT,
    ISIN TEXT,
    指数分類コード TEXT,
    指数分類 TEXT,
    業種コード TEXT,
    業種区分 TEXT,
    規模1コード TEXT,
    規模1区分 TEXT,
    規模2コード TEXT,
    規模2区分 TEXT,
    指数用株価終値 REAL,
    指数用株式数 REAL,
    指数用配当金 REAL,
    指数用配当金総額 REAL,
    株式数比較 REAL,
    最終時価総額 REAL,
    売買単位 INTEGER,
    FFW REAL,
    [指数用株式数（100％型）] REAL,
    基礎価格 REAL,
    PRIMARY KEY (日付, 銘柄コード)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS change_in_constituents (
    更新コード TEXT,
    更新区分 TEXT,
    情報登録日 TEXT,
    所報掲載日 TEXT,
    銘柄コード TEXT,
    銘柄名 TEXT,
    ISIN TEXT,
    指数分類コード（情報登録日） TEXT,
    指数分類（情報登録日） TEXT,
    指数修正日 TEXT,
    事象コード TEXT,
    事象 TEXT,
    割当率1 REAL,
    割当率2 REAL,
    払込金 REAL,
    追加指数用株式数 REAL,
    指数分類コード（指数修正日） TEXT,
    指数分類（指数修正日時点） TEXT,
    新業種コード TEXT,
    新業種区分 TEXT,
    新規模2コード TEXT,
    新規模2区分 TEXT,
    訂正前情報登録日 TEXT,
    予備フィールド TEXT,
    新FFW REAL,
    旧FFW REAL,
    新売買単位 INTEGER,
    旧売買単位 INTEGER,
    旧業種コード TEXT,
    旧業種区分 TEXT,
    旧規模2コード TEXT,
    旧規模2区分 TEXT,
    追加指数用株式数（100％型） REAL,
    指数用株式数（修正日時点） REAL,
    指数用株式数（100％型）（修正日時点） REAL
);
""")

conn.commit()

# 指数マスタのCSV取り込み
master_files = glob.glob(os.path.join(master_csv_dir, '*.csv'))
for file in master_files:
    try:
        df = pd.read_csv(file, dtype=str, encoding='cp932')
        # 型変換（列名が合わない場合はここでrenameすること）
        for col in ['指数用株価終値', '指数用株式数', '指数用配当金', '指数用配当金総額',
                    '株式数比較', '最終時価総額', '売買単位', 'FFW', '指数用株式数（100％型）', '基礎価格']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df.to_sql('master_of_index', conn, if_exists='append', index=False)
        print(f'取込完了: {file}')
    except Exception as e:
        print(f'エラー: {file} - {e}')

# 指数用株式数等変更（予告）のCSV取り込み
change_files = glob.glob(os.path.join(change_csv_dir, '*.csv'))
for file in change_files:
    try:
        df = pd.read_csv(file, dtype=str, encoding='cp932')
        for col in ['割当率1', '割当率2', '払込金', '追加指数用株式数', '新FFW', '旧FFW', '新売買単位', '旧売買単位',
                    '追加指数用株式数（100％型）', '指数用株式数（修正日時点）', '指数用株式数（100％型）（修正日時点）']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df.to_sql('change_in_constituents', conn, if_exists='append', index=False)
        print(f'取込完了: {file}')
    except Exception as e:
        print(f'エラー: {file} - {e}')

conn.close()
print('全CSVの取込が完了しました。')
