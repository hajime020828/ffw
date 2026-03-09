import os
import csv
import sqlite3
import shutil
import re
from glob import glob
from datetime import datetime

# --- 設定 ---
DATA_DIR = r"H:\backup-data\大株主30位"
CSV_GLOB = os.path.join(DATA_DIR, "OKQA3_*.csv")
DB_PATH = os.path.join(DATA_DIR, "major_shareholders.db")
BACKUP_FMT = os.path.join(DATA_DIR, "major_shareholders.db.bak_{ts}")
BAD_ROWS_CSV = os.path.join(DATA_DIR, "bad_rows_all.csv")
EXPORT_CSV = os.path.join(DATA_DIR, "major_shareholders_export.csv")
ENCODING = "cp932"

# CSV 列インデックス（0-based）
IDX_COMPANY = 0
IDX_SETTLEMENT_TERM = 1
IDX_SETTLEMENT_FLAG = 2
IDX_DATA_ID = 3
IDX_NAME_EN = 4
IDX_NAME_JP = 5
IDX_SHAREHOLDER_CODE = 6
IDX_SHAREHOLDER_ATTR_FLAG = 7
IDX_INFO_FLAG = 8
IDX_SHARES = 9
IDX_PCT = 10

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS major_shareholders (
    company TEXT NOT NULL,
    settlement_term TEXT NOT NULL,
    settlement_flag INTEGER NOT NULL,
    data_id INTEGER NOT NULL,
    name_en TEXT,
    name_jp TEXT,
    shareholder_code TEXT,
    shareholder_attr_flag TEXT,
    info_flag TEXT,
    shares INTEGER,
    pct REAL,
    PRIMARY KEY (company, settlement_term, settlement_flag, data_id)
);
"""

# --- ヘルパー ---
def backup_db(path):
    if os.path.exists(path):
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        dest = BACKUP_FMT.format(ts=ts)
        shutil.copy2(path, dest)
        print("Backed up DB to:", dest)

def norm_text(s):
    if s is None:
        return None
    s = str(s).strip()
    return s if s != "" else None

def parse_int(s):
    if s is None:
        return None
    s = str(s).strip().replace(",","")
    if s == "":
        return None
    try:
        return int(float(s))
    except:
        return None

def parse_float(s):
    if s is None:
        return None
    s = str(s).strip().replace(",","").replace("%","")
    if s == "":
        return None
    try:
        return float(s)
    except:
        return None

RE_SHAREHOLDER_CODE = re.compile(r'^\d{6}$')
RE_ATTR_FLAG = re.compile(r'^[A-Za-z]$')

def is_valid_tail(code, attr_flag, info_flag, shares, pct):
    cond_code = True if (code is None or RE_SHAREHOLDER_CODE.match(code)) else False
    cond_attr = True if (attr_flag is None or RE_ATTR_FLAG.match(attr_flag)) else False
    cond_num = (parse_int(shares) is not None) or (parse_float(pct) is not None)
    return cond_code and cond_attr and cond_num

def try_fix_shifts_row(row):
    # ensure length
    if len(row) <= IDX_PCT:
        row = row + [""] * (IDX_PCT + 1 - len(row))
    m = [norm_text(c) for c in row[:IDX_PCT+1]]

    # quick valid?
    if is_valid_tail(m[IDX_SHAREHOLDER_CODE], m[IDX_SHAREHOLDER_ATTR_FLAG], m[IDX_INFO_FLAG], m[IDX_SHARES], m[IDX_PCT]):
        return m, "ok"

    # right shift
    r = m.copy()
    r[IDX_PCT] = r[IDX_SHARES]
    r[IDX_SHARES] = r[IDX_INFO_FLAG]
    r[IDX_INFO_FLAG] = r[IDX_SHAREHOLDER_ATTR_FLAG]
    r[IDX_SHAREHOLDER_ATTR_FLAG] = r[IDX_SHAREHOLDER_CODE]
    r[IDX_SHAREHOLDER_CODE] = None
    if is_valid_tail(r[IDX_SHAREHOLDER_CODE], r[IDX_SHAREHOLDER_ATTR_FLAG], r[IDX_INFO_FLAG], r[IDX_SHARES], r[IDX_PCT]):
        return r, "right_shift"

    # left shift
    l = m.copy()
    l[IDX_SHAREHOLDER_CODE] = l[IDX_SHAREHOLDER_ATTR_FLAG]
    l[IDX_SHAREHOLDER_ATTR_FLAG] = l[IDX_INFO_FLAG]
    l[IDX_INFO_FLAG] = l[IDX_SHARES]
    l[IDX_SHARES] = l[IDX_PCT]
    l[IDX_PCT] = None
    if is_valid_tail(l[IDX_SHAREHOLDER_CODE], l[IDX_SHAREHOLDER_ATTR_FLAG], l[IDX_INFO_FLAG], l[IDX_SHARES], l[IDX_PCT]):
        return l, "left_shift"

    # heuristic: pct looks like large int -> treat as shares
    try:
        pctv = parse_float(m[IDX_PCT])
        shv = parse_int(m[IDX_SHARES])
        if pctv is not None and pctv > 1000 and shv is None:
            m2 = m.copy()
            m2[IDX_SHARES] = m[IDX_PCT]
            m2[IDX_PCT] = None
            if is_valid_tail(m2[IDX_SHAREHOLDER_CODE], m2[IDX_SHAREHOLDER_ATTR_FLAG], m2[IDX_INFO_FLAG], m2[IDX_SHARES], m2[IDX_PCT]):
                return m2, "pct_to_shares"
    except:
        pass

    return None, "unfixable"

# --- メイン処理（スキップモード） ---
def process_all_csvs_skip_mode():
    files = sorted(glob(CSV_GLOB))
    if not files:
        print("No CSV files found.", CSV_GLOB)
        return

    backup_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    bad_writer = open(BAD_ROWS_CSV, "w", encoding="utf-8", newline="")
    bw = csv.writer(bad_writer)
    bw.writerow(["source_file","row_num","reason","fix_reason","company","settlement_term","settlement_flag","data_id","name_en","name_jp","shareholder_code","shareholder_attr_flag","info_flag","shares","pct","raw_row"])

    total_inserted = 0
    total_skipped = 0
    total_bad = 0

    for csv_file in files:
        print("Processing:", os.path.basename(csv_file))
        ins = 0
        skip = 0
        bad = 0
        with open(csv_file, "r", encoding=ENCODING, errors="replace", newline="") as f:
            reader = csv.reader(f)
            for i,row in enumerate(reader, start=1):
                if not row or all((str(c).strip()=="") for c in row):
                    continue
                # pad
                if len(row) <= IDX_PCT:
                    row = row + [""] * (IDX_PCT + 1 - len(row))
                # head
                company = norm_text(row[IDX_COMPANY])
                settlement_term = norm_text(row[IDX_SETTLEMENT_TERM])
                try:
                    settlement_flag = int(norm_text(row[IDX_SETTLEMENT_FLAG]) or 0)
                except:
                    settlement_flag = 0
                try:
                    data_id = int(norm_text(row[IDX_DATA_ID]) or 0)
                except:
                    data_id = 0
                name_en = norm_text(row[IDX_NAME_EN])
                name_jp = norm_text(row[IDX_NAME_JP])

                fixed, reason = try_fix_shifts_row(row)
                if fixed is None:
                    bw.writerow([os.path.basename(csv_file), i, "unfixable", reason, company, settlement_term, settlement_flag, data_id, name_en, name_jp, "", "", "", "", "", "|".join(row)])
                    bad += 1
                    continue

                shareholder_code = fixed[IDX_SHAREHOLDER_CODE]
                shareholder_attr_flag = fixed[IDX_SHAREHOLDER_ATTR_FLAG]
                info_flag = fixed[IDX_INFO_FLAG]
                shares = parse_int(fixed[IDX_SHARES])
                pct = parse_float(fixed[IDX_PCT])

                if shares is None and pct is None:
                    bw.writerow([os.path.basename(csv_file), i, "no_numeric", reason, company, settlement_term, settlement_flag, data_id, name_en, name_jp, shareholder_code, shareholder_attr_flag, info_flag, shares, pct, "|".join(row)])
                    bad += 1
                    continue

                # skip if exists
                cur = conn.execute("SELECT 1 FROM major_shareholders WHERE company=? AND settlement_term=? AND settlement_flag=? AND data_id=? LIMIT 1", (company, settlement_term, settlement_flag, data_id))
                if cur.fetchone() is not None:
                    skip += 1
                else:
                    try:
                        conn.execute("""
                        INSERT INTO major_shareholders
                        (company, settlement_term, settlement_flag, data_id, name_en, name_jp, shareholder_code, shareholder_attr_flag, info_flag, shares, pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (company, settlement_term, settlement_flag, data_id, name_en, name_jp, shareholder_code, shareholder_attr_flag, info_flag, shares, pct))
                        ins += 1
                    except Exception as e:
                        bw.writerow([os.path.basename(csv_file), i, f"db_error:{e}", reason, company, settlement_term, settlement_flag, data_id, name_en, name_jp, shareholder_code, shareholder_attr_flag, info_flag, shares, pct, "|".join(row)])
                        bad += 1
        conn.commit()
        print(f"  inserted={ins}, skipped={skip}, bad={bad}")
        total_inserted += ins
        total_skipped += skip
        total_bad += bad

    bad_writer.close()
    conn.close()

    print(f"All done. total inserted={total_inserted}, total skipped={total_skipped}, total bad={total_bad}. Bad rows -> {BAD_ROWS_CSV}")

    # export DB
    conn2 = sqlite3.connect(DB_PATH)
    cur = conn2.cursor()
    cur.execute("SELECT company, settlement_term, settlement_flag, data_id, name_en, name_jp, shareholder_code, shareholder_attr_flag, info_flag, shares, pct FROM major_shareholders ORDER BY company, settlement_term, data_id")
    rows = cur.fetchall()
    conn2.close()
    with open(EXPORT_CSV, "w", encoding="cp932", newline="") as ef:
        w = csv.writer(ef)
        w.writerow(["company","settlement_term","settlement_flag","data_id","name_en","name_jp","shareholder_code","shareholder_attr_flag","info_flag","shares","pct"])
        for r in rows:
            w.writerow([ "" if v is None else v for v in r ])
    print("Exported DB to:", EXPORT_CSV)

if __name__ == "__main__":
    process_all_csvs_skip_mode()
