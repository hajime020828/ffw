import os
import csv
import sqlite3

# ここにCSVファイルパス（1つだけ指定してください）
CSV_PATH = r"H:\backup-data\大株主30位\大株主30位過去データ\OKQA3_202512.csv"

DB_PATH = r"H:\backup-data\大株主30位\major_shareholders_new.db"
ENCODING = "cp932"

# 列インデックス
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

def insert_or_update_from_csv_with_updated_cols(csv_path):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    total_error = 0
    total_rows = 0
    error_rows = []
    updated_detail_list = []

    with open(csv_path, "r", encoding=ENCODING, errors="replace", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            # 空行スキップ
            if not row or all((str(c).strip()=="") for c in row):
                continue
            total_rows += 1
            if len(row) <= IDX_PCT:
                row = row + [""] * (IDX_PCT + 1 - len(row))
            try:
                company = norm_text(row[IDX_COMPANY])
                settlement_term = norm_text(row[IDX_SETTLEMENT_TERM])
                try:
                    settlement_flag = int(norm_text(row[IDX_SETTLEMENT_FLAG]) or 0)
                except Exception as e:
                    raise ValueError(f"settlement_flag parse error: {e}")
                try:
                    data_id = int(norm_text(row[IDX_DATA_ID]) or 0)
                except Exception as e:
                    raise ValueError(f"data_id parse error: {e}")
                name_en = norm_text(row[IDX_NAME_EN])
                name_jp = norm_text(row[IDX_NAME_JP])
                shareholder_code = norm_text(row[IDX_SHAREHOLDER_CODE])
                shareholder_attr_flag = norm_text(row[IDX_SHAREHOLDER_ATTR_FLAG])
                info_flag = norm_text(row[IDX_INFO_FLAG])
                shares = parse_int(row[IDX_SHARES])
                pct = parse_float(row[IDX_PCT])
            except Exception as e:
                total_error += 1
                error_rows.append([i+1, str(e), row])
                continue

            # 主キー
            pk = (company, settlement_term, settlement_flag, data_id)
            # DBに既存レコードがあるか
            try:
                cur = conn.execute(
                    """SELECT name_en, name_jp, shareholder_code, shareholder_attr_flag, info_flag, shares, pct 
                       FROM major_shareholders WHERE company=? AND settlement_term=? AND settlement_flag=? AND data_id=? LIMIT 1""", pk
                )
                exists = cur.fetchone()
            except Exception as e:
                total_error += 1
                error_rows.append([i+1, f"DB select error: {e}", row])
                continue

            col_names = ["name_en", "name_jp", "shareholder_code", "shareholder_attr_flag", "info_flag", "shares", "pct"]
            csv_vals = [name_en, name_jp, shareholder_code, shareholder_attr_flag, info_flag, shares, pct]

            if exists is None:
                # 新規INSERT
                try:
                    conn.execute("""
                    INSERT INTO major_shareholders
                    (company, settlement_term, settlement_flag, data_id, name_en, name_jp, shareholder_code, shareholder_attr_flag, info_flag, shares, pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (company, settlement_term, settlement_flag, data_id, *csv_vals))
                    total_inserted += 1
                except Exception as e:
                    total_error += 1
                    error_rows.append([i+1, f"DB insert error: {e}", row])
                    continue
            else:
                # 欠損カラムのみUPDATEで補完
                sets = []
                params = []
                updated_cols = []
                for col, exist, new in zip(col_names, exists, csv_vals):
                    if (exist is None or (isinstance(exist, str) and exist.strip() == "") or (isinstance(exist, (int, float)) and exist == 0)):
                        if new not in [None, "", 0]:
                            sets.append(f"{col}=?")
                            params.append(new)
                            updated_cols.append(col)
                if sets:
                    try:
                        sql = f"UPDATE major_shareholders SET {', '.join(sets)} WHERE company=? AND settlement_term=? AND settlement_flag=? AND data_id=?"
                        params += [company, settlement_term, settlement_flag, data_id]
                        conn.execute(sql, params)
                        total_updated += 1
                        updated_detail_list.append([
                            i+1, company, settlement_term, settlement_flag, data_id, ";".join(updated_cols)
                        ])
                    except Exception as e:
                        total_error += 1
                        error_rows.append([i+1, f"DB update error: {e}", row])
                        continue
                else:
                    total_skipped += 1

    conn.commit()
    conn.close()

    print(f"\nファイル: {os.path.basename(csv_path)}")
    print(f"  全データ行数: {total_rows}")
    print(f"  新規: {total_inserted}")
    print(f"  補完更新: {total_updated}")
    print(f"  重複スキップ: {total_skipped}")
    print(f"  エラー: {total_error}")

    expected = total_inserted + total_updated + total_skipped + total_error
    if expected != total_rows:
        print(f"【警告】件数が一致しません！合計={expected}、CSV行数={total_rows}")
    else:
        print("件数一致（取り込み漏れなし）")

    # 補完更新カラムをCSV出力
    if total_updated > 0:
        outcsv = os.path.splitext(csv_path)[0] + "_updated_cols.csv"
        with open(outcsv, "w", encoding="utf-8-sig", newline="") as wf:
            w = csv.writer(wf)
            w.writerow(["rownum","company","term","settlement_flag","data_id","updated_cols"])
            w.writerows(updated_detail_list)
        print(f"  補完更新の詳細リスト: {outcsv}")

    # エラー行も記録
    if total_error > 0:
        err_path = os.path.splitext(csv_path)[0] + "_error_rows.csv"
        with open(err_path, "w", encoding="utf-8-sig", newline="") as wf:
            w = csv.writer(wf)
            w.writerow(["rownum","error","raw_row"])
            w.writerows(error_rows)
        print(f"  エラー行リスト: {err_path}")

if __name__ == "__main__":
    if not os.path.isfile(CSV_PATH):
        print(f"ファイルが見つかりません: {CSV_PATH}")
    else:
        insert_or_update_from_csv_with_updated_cols(CSV_PATH)
        print("完了しました。")
