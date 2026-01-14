import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import path from 'path';

export default async function handler(req, res) {
  const { company } = req.query;

  // __dirnameはESMでは使えないので、process.cwd()でカレントパスを取得
  const dbPath = path.join(process.cwd(), 'major_shareholders.db');
  const db = await open({ filename: dbPath, driver: sqlite3.Database });

  const rows = await db.all(`
    SELECT settlement_term, data_id, name_en, shares, pct
    FROM major_shareholders
    WHERE company = ?
      AND ((data_id BETWEEN 1 AND 35) OR (data_id BETWEEN 50 AND 61))
    ORDER BY settlement_term ASC, data_id ASC
  `, company);

  res.status(200).json(rows);
}
