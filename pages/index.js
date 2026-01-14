import { useState, useEffect } from "react";

const AGGREGATE_LABELS = {
  50: "Number of Shareholders",
  51: "Number of Unit Shareholders",
  52: "Shares Issued",
  53: "Foreigners",
  54: "Major Shareholders",
  55: "Floating Shares",
  56: "Investment Trusts",
  57: "Pension Trusts",
  58: "Employee Stock Ownership Plan",
  59: "Officers",
  60: "Top 10 Shareholders",
  61: "Treasury Stock"
};

// 小数第6位四捨五入
function round6(num) {
  return Math.round(num * 1e6) / 1e6;
}
// step刻みで切り上げ
function ceilStep(num, step = 0.05) {
  return Math.ceil(num / step) * step;
}

export default function Home() {
  const [company, setCompany] = useState("1301");
  const [data, setData] = useState([]);

  useEffect(() => {
    if (!company) return;
    fetch(`/api/major_shareholders?company=${company}`)
      .then(res => res.json())
      .then(setData);
  }, [company]);

  // settlement_termごとにグループ化
  const grouped = {};
  data.forEach(row => {
    if (!grouped[row.settlement_term]) grouped[row.settlement_term] = [];
    grouped[row.settlement_term].push(row);
  });

  const terms = Object.keys(grouped).sort();

  // サマリー集計
  // termごとに"fixedRatio" "floatRatioRaw" "ffwRatio" などを計算
  const summaryByTerm = {};
  terms.forEach(term => {
    const issued = grouped[term].find(r => r.data_id === 52)?.shares ?? null;
    const major = grouped[term].find(r => r.data_id === 54)?.shares ?? null;
    if (issued && major) {
      const fixedRatio = round6(major / issued);
      const floatRatioRaw = round6(1 - fixedRatio);
      const ffwRatio = ceilStep(floatRatioRaw, 0.05);
      summaryByTerm[term] = {
        fixedRatio,
        floatRatioRaw,
        ffwRatio,
        issued,
        major
      };
    }
  });

  // FFW区分ビン
  const bins = [];
  for (let f = 0.05; f <= 1.00; f += 0.05) {
    bins.push(Number(f.toFixed(2)));
  }

  // 1～35位: どこかのtermに株主がいれば表示
  const rankData = {};
  for (let i = 1; i <= 35; i++) {
    rankData[i] = {};
    terms.forEach(term => {
      const row = grouped[term]?.find(r => r.data_id === i);
      if (row && row.name_en) rankData[i][term] = row;
    });
  }
  const ranksToShow = Object.entries(rankData)
    .filter(([, termMap]) => Object.keys(termMap).length > 0)
    .map(([rank]) => Number(rank));

  // 50～61: どこかのtermに値があれば表示
  const aggrData = {};
  for (let i = 50; i <= 61; i++) {
    aggrData[i] = {};
    terms.forEach(term => {
      const row = grouped[term]?.find(r => r.data_id === i);
      if (row && row.name_en) aggrData[i][term] = row;
    });
  }
  const aggrToShow = Object.entries(aggrData)
    .filter(([, termMap]) => Object.keys(termMap).length > 0)
    .map(([i]) => Number(i));

  return (
    <div>
      <h1>大株主時系列比較・集計値付き</h1>
      <label>
        銘柄コード: 
        <input value={company} onChange={e => setCompany(e.target.value)} />
      </label>

      {/* サマリー表示 */}
      <h2>サマリー（固定株比率/浮動株比率/FFW浮動株比率 推移）</h2>
      <table border={1}>
       <thead>
        <tr>
          <th>期</th>
          <th>固定株比率<br/>(major / issued)</th>
          <th>浮動株比率<br/>(1 - 固定株比率, 切り上げ前)</th>
          <th>FFW浮動株比率<br/>(0.05切り上げ)</th>
        </tr>
       </thead>
      <tbody>
          {terms.map(term => summaryByTerm[term] && (
          <tr key={term}>
            <td>{term}</td>
            <td>
            {summaryByTerm[term].fixedRatio}
            <br/>
            ({summaryByTerm[term].major} / {summaryByTerm[term].issued})
            </td>
            <td>
            {summaryByTerm[term].floatRatioRaw}
            </td>
            <td>
            {summaryByTerm[term].ffwRatio.toFixed(2)}
            </td>
        </tr>
        ))}
    </tbody>
    </table>

      <h2>FFW区分テーブル</h2>
      <table border={1}>
        <thead>
          <tr>
            <th>区分</th>
            {terms.map(term => <th key={term}>{term}</th>)}
          </tr>
        </thead>
        <tbody>
          {bins.map(bin => (
            <tr key={bin}>
              <td>{bin.toFixed(2)}</td>
              {terms.map(term => {
                const ffw = summaryByTerm[term]?.ffwRatio ?? null;
                return (
                  <td key={term} style={{background: ffw === bin ? "#ffe0e0" : undefined}}>
                    {ffw === bin ? "●" : ""}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>

      <div style={{overflowX:'auto', marginTop: 20}}>
        <table border={1}>
          <thead>
            <tr>
              <th>順位/項目</th>
              {terms.map(term => (
                <th key={term}>{term}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {/* 1~35位 */}
            {ranksToShow.map(i => (
              <tr key={i}>
                <td>{i}位</td>
                {terms.map(term => {
                  const row = rankData[i][term];
                  return (
                    <td key={term}>
                      {row
                        ? <>
                            {row.name_en}<br/>
                            {row.shares}株 ({row.pct}%)
                          </>
                        : "-"
                      }
                    </td>
                  );
                })}
              </tr>
            ))}
            {/* 罫線 */}
            {aggrToShow.length > 0 && (
              <tr>
                <td colSpan={terms.length + 1}><hr /></td>
              </tr>
            )}
            {/* 50~61の集計項目 */}
            {aggrToShow.map(i => (
              <tr key={i}>
                <td>{AGGREGATE_LABELS[i] || `${i}`}</td>
                {terms.map(term => {
                  const row = aggrData[i][term];
                  return (
                    <td key={term}>
                      {row
                        ? <>
                            {row.shares}{row.shares !== null ? "株" : ""}
                            {typeof row.pct === 'number' ? ` (${row.pct}%)` : ""}
                          </>
                        : "-"
                      }
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
