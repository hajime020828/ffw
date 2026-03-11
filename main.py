from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import sqlite3
import os
import math

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = r"C:\Users\165000\Workspace\topix_ffw_projection"
DATA_DIR = os.path.join(BASE_DIR, "data")
SHAREHOLDER_DB = os.path.join(DATA_DIR, "major_shareholders.db")
TOPIX_DB = os.path.join(DATA_DIR, "topix_index.db")


def init_db():
    conn = sqlite3.connect(SHAREHOLDER_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_overrides (
            company TEXT,
            settlement_term TEXT,
            data_id INTEGER,
            is_fixed INTEGER DEFAULT 0,
            override_shares INTEGER,
            PRIMARY KEY (company, settlement_term, data_id)
        )
    """)
    conn.commit()
    conn.close()


init_db()


def get_db_connection(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_target_topix_month(settlement_term: str) -> str:
    year = int(settlement_term[:4])
    month = int(settlement_term[4:6])
    if 1 <= month <= 3:
        return f"{year}10"
    elif 4 <= month <= 6:
        return f"{year + 1}01"
    elif 7 <= month <= 9:
        return f"{year + 1}04"
    elif 10 <= month <= 12:
        return f"{year + 1}07"
    else:
        raise ValueError(f"Invalid month: {month}")


# --- Ticker list (deduped) ---
@app.get("/api/tickers")
def get_tickers():
    conn = get_db_connection(SHAREHOLDER_DB)
    cur = conn.cursor()

    # company を一意にする（ここが重要：React key 重複防止）
    cur.execute("""
        SELECT
            company,
            MAX(COALESCE(name_jp, '')) AS name_jp,
            MAX(COALESCE(name_en, '')) AS name_en
        FROM major_shareholders
        WHERE data_id = 0
        GROUP BY company
        ORDER BY company
    """)
    rows = cur.fetchall()
    conn.close()

    return [{"code": r["company"], "name_jp": r["name_jp"], "name_en": r["name_en"]} for r in rows]


@app.get("/api/shareholders/{company}")
def get_company_history(company: str):
    conn = get_db_connection(SHAREHOLDER_DB)
    cur = conn.cursor()

    cur.execute(
        "SELECT name_en, name_jp FROM major_shareholders WHERE company = ? AND data_id = 0 LIMIT 1",
        (company,),
    )
    name_row = cur.fetchone()
    company_name_en = name_row["name_en"] if name_row and name_row["name_en"] else ""
    company_name_jp = name_row["name_jp"] if name_row and name_row["name_jp"] else ""

    cur.execute("""
        SELECT settlement_term, MIN(settlement_flag) as flag
        FROM major_shareholders
        WHERE company = ?
        GROUP BY settlement_term
        ORDER BY settlement_term ASC
    """, (company,))
    terms_info = [{"term": row["settlement_term"], "flag": row["flag"]} for row in cur.fetchall()]

    if not terms_info:
        conn.close()
        raise HTTPException(status_code=404, detail="Company not found")

    history = []
    for t_info in terms_info:
        term = t_info["term"]
        flag = t_info["flag"]

        query = """
        SELECT
            m.company, m.settlement_term, m.data_id, m.name_jp, m.name_en, m.shares, m.pct,
            u.is_fixed as user_fixed,
            u.override_shares
        FROM major_shareholders m
        LEFT JOIN user_overrides u
            ON m.company = u.company
            AND m.settlement_term = u.settlement_term
            AND m.data_id = u.data_id
        WHERE m.company = ? AND m.settlement_term = ? AND m.data_id > 0
        ORDER BY m.data_id ASC
        """
        cur.execute(query, (company, term))
        shareholders_raw = [dict(row) for row in cur.fetchall()]

        top10_shares = [
            s["shares"]
            for s in shareholders_raw
            if 1 <= s["data_id"] <= 10 and s["shares"] is not None
        ]

        shareholders = []
        for s in shareholders_raw:
            is_fixed = s["user_fixed"]
            if is_fixed is None:
                if 1 <= s["data_id"] <= 10:
                    is_fixed = 1
                elif s["data_id"] == 59:
                    is_fixed = 0
                elif s["data_id"] == 61:
                    is_fixed = 0 if s["shares"] in top10_shares else 1
                else:
                    is_fixed = 0

            s_out = dict(s)
            s_out["is_fixed"] = is_fixed
            del s_out["user_fixed"]
            shareholders.append(s_out)

        target_month_str = get_target_topix_month(term)
        topix_code = f"{company}0"

        actual_ffw, actual_ffw_date = None, None
        if os.path.exists(TOPIX_DB):
            t_conn = get_db_connection(TOPIX_DB)
            t_cur = t_conn.cursor()
            t_cur.execute("""
                SELECT 日付, FFW
                FROM master_of_index
                WHERE 銘柄コード = ?
                  AND REPLACE(REPLACE(日付, '/', ''), '-', '') LIKE ?
                ORDER BY 日付 DESC LIMIT 1
            """, (topix_code, f"{target_month_str}%"))
            topix_row = t_cur.fetchone()
            t_conn.close()
            if topix_row:
                actual_ffw = topix_row["FFW"]
                actual_ffw_date = topix_row["日付"]

        history.append({
            "settlement_term": term,
            "settlement_flag": flag,
            "target_topix_month": target_month_str,
            "actual_ffw": actual_ffw,
            "actual_ffw_date": actual_ffw_date,
            "shareholders": shareholders,
        })

    conn.close()

    return {
        "company": company,
        "company_name_en": company_name_en,
        "company_name_jp": company_name_jp,
        "history": history,
    }


class OverrideItem(BaseModel):
    data_id: int
    is_fixed: int
    override_shares: Optional[int] = None


class OverrideRequest(BaseModel):
    company: str
    settlement_term: str
    overrides: List[OverrideItem]


@app.post("/api/overrides")
def save_overrides(req: OverrideRequest):
    conn = sqlite3.connect(SHAREHOLDER_DB)
    cur = conn.cursor()

    for item in req.overrides:
        cur.execute("""
            INSERT INTO user_overrides (company, settlement_term, data_id, is_fixed, override_shares)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(company, settlement_term, data_id)
            DO UPDATE SET
                is_fixed = excluded.is_fixed,
                override_shares = excluded.override_shares
        """, (req.company, req.settlement_term, item.data_id, item.is_fixed, item.override_shares))

    conn.commit()
    conn.close()
    return {"status": "success"}


@app.get("/api/summary")
def get_summary_list():
    topix_dict = {}
    valid_companies = set()

    if os.path.exists(TOPIX_DB):
        t_conn = get_db_connection(TOPIX_DB)
        t_cur = t_conn.cursor()

        t_cur.execute("SELECT MAX(日付) as max_date FROM master_of_index")
        max_date_row = t_cur.fetchone()
        max_date = max_date_row["max_date"] if max_date_row else None

        if max_date:
            try:
                t_cur.execute("""
                    SELECT 銘柄コード FROM master_of_index
                    WHERE 日付 = ?
                      AND (規模2区分 LIKE '%TOPIX%' OR 規模2区分 LIKE '%ＴＯＰＩＸ%')
                """, (max_date,))
                for row in t_cur.fetchall():
                    code5 = row["銘柄コード"]
                    if code5 and len(code5) >= 4:
                        valid_companies.add(code5[:-1])
            except sqlite3.OperationalError:
                pass

        t_cur.execute("SELECT 銘柄コード, 日付, FFW FROM master_of_index ORDER BY 日付 ASC")
        for row in t_cur.fetchall():
            code5 = row["銘柄コード"]
            if not code5 or len(code5) < 4:
                continue
            company = code5[:-1]
            date_str = row["日付"]
            if not date_str:
                continue
            clean_date = date_str.replace("/", "").replace("-", "")
            if len(clean_date) < 6:
                continue
            yyyymm = clean_date[:6]
            topix_dict.setdefault(company, {})[yyyymm] = row["FFW"]

        t_conn.close()

    conn = get_db_connection(SHAREHOLDER_DB)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            m.company, m.settlement_term, m.data_id, m.shares, m.name_en, m.name_jp,
            u.is_fixed as user_fixed, u.override_shares
        FROM major_shareholders m
        LEFT JOIN user_overrides u
            ON m.company = u.company AND m.settlement_term = u.settlement_term AND m.data_id = u.data_id
        WHERE m.settlement_flag = 0
    """)
    raw_data = cur.fetchall()
    conn.close()

    shareholder_dict = {}
    company_names = {}

    for row in raw_data:
        c = row["company"]
        if valid_companies and c not in valid_companies:
            continue
        if c not in topix_dict:
            continue

        t = row["settlement_term"]
        d = row["data_id"]

        if d == 0:
            company_names.setdefault(c, {
                "en": row["name_en"] if row["name_en"] else "",
                "jp": row["name_jp"] if row["name_jp"] else "",
            })
            continue

        shareholder_dict.setdefault(c, {}).setdefault(t, []).append(dict(row))

    results = []

    for c, terms_data in shareholder_dict.items():
        history = []
        for t in sorted(terms_data.keys()):
            rows = terms_data[t]

            total_shares = 0
            fixed_shares = 0

            top10_shares = []
            for r in rows:
                if 1 <= r["data_id"] <= 10:
                    val = r["override_shares"] if r["override_shares"] is not None else r["shares"]
                    if val is not None:
                        top10_shares.append(val)

            for r in rows:
                if r["data_id"] == 52:
                    total_shares = r["override_shares"] if r["override_shares"] is not None else r["shares"]
                    if total_shares is None:
                        total_shares = 0
                    break

            if total_shares > 0:
                for r in rows:
                    d = r["data_id"]
                    if d in (0, 52):
                        continue

                    val = r["override_shares"] if r["override_shares"] is not None else r["shares"]
                    if val is None:
                        val = 0

                    is_fixed = r["user_fixed"]
                    if is_fixed is None:
                        if 1 <= d <= 10:
                            is_fixed = 1
                        elif d == 59:
                            is_fixed = 0
                        elif d == 61:
                            is_fixed = 0 if val in top10_shares else 1
                        else:
                            is_fixed = 0

                    if is_fixed == 1 and (d < 50 or d in (59, 61)):
                        fixed_shares += val

                calc_ffw = 1 - (fixed_shares / total_shares)
                rounded_calc_ffw = math.ceil(round(calc_ffw * 20, 6)) / 20.0
            else:
                calc_ffw = None
                rounded_calc_ffw = None

            target_month = get_target_topix_month(t)
            actual_ffw = topix_dict.get(c, {}).get(target_month)
            diff = (rounded_calc_ffw - actual_ffw) if (rounded_calc_ffw is not None and actual_ffw is not None) else None

            history.append({
                "term": t,
                "effective": target_month,
                "calc_ffw": calc_ffw,
                "rounded_calc_ffw": rounded_calc_ffw,
                "actual_ffw": actual_ffw,
                "diff": diff,
            })

        results.append({
            "company": c,
            "name_en": company_names.get(c, {}).get("en", ""),
            "name_jp": company_names.get(c, {}).get("jp", ""),
            "history": history,
        })

    return {"data": results}
