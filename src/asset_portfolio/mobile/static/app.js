const { useEffect, useMemo, useState } = React;

// ✅ 숫자 포맷을 보기 좋게 만드는 유틸 함수입니다.
const formatNumber = (value, options = {}) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return new Intl.NumberFormat("ko-KR", options).format(value);
};

// ✅ 금액/퍼센트 표시에 사용합니다.
const formatCurrency = (value) => formatNumber(value, { maximumFractionDigits: 0 });
const formatPercent = (value) => formatNumber(value, { maximumFractionDigits: 2 });

// ✅ 쿼리스트링에서 mock=1이면 서버 대신 샘플 데이터를 보여줍니다.
const isMockMode = new URLSearchParams(window.location.search).get("mock") === "1";

const mockData = {
  kpi: {
    total_valuation: 125000000,
    total_purchase: 98000000,
    profit: 27000000,
    profit_rate: 27.55,
    portfolio_return_pct: 12.34,
  },
  snapshot: {
    latest_date: "2025-02-01",
    rows: [
      {
        account_name: "Main",
        asset_name: "삼성전자",
        quantity: 12,
        purchase_price: 65000,
        valuation_price: 72000,
        manual_principal: null,
        valuation_amount: 864000,
        profit_amount: 84000,
        profit_rate: 10.78,
        currency: "KRW",
        asset_type: "주식",
      },
    ],
  },
  transactions: {
    rows: [
      {
        transaction_date: "2025-02-01",
        trade_type: "매수",
        ticker: "005930",
        asset_name: "삼성전자",
        asset_currency: "KRW",
        quantity: 2,
        price: 72000,
        fee: 100,
        tax: 0,
        account_name: "Main",
        memo: "모바일 테스트",
      },
    ],
  },
  contributions: {
    rows: [
      { asset_id: 1, name_kr: "삼성전자", cum_contribution_pct: 4.12 },
      { asset_id: 2, name_kr: "애플", cum_contribution_pct: 3.78 },
    ],
  },
  treemap: {
    latest_date: "2025-02-01",
    rows: [
      { asset_id: 1, name_kr: "삼성전자", asset_type: "주식", market: "KR", value: 12000000 },
      { asset_id: 2, name_kr: "애플", asset_type: "주식", market: "US", value: 15000000 },
    ],
  },
};

const App = () => {
  const [accounts, setAccounts] = useState([]);
  const [accountId, setAccountId] = useState("__ALL__");
  const [days, setDays] = useState(30);
  const [topK, setTopK] = useState(5);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [kpi, setKpi] = useState(null);
  const [snapshot, setSnapshot] = useState(null);
  const [transactions, setTransactions] = useState([]);
  const [contributions, setContributions] = useState([]);
  const [treemap, setTreemap] = useState({ latest_date: null, rows: [] });

  // ✅ 최초 진입 시 계좌 목록을 가져옵니다.
  useEffect(() => {
    if (isMockMode) {
      setAccounts([{ id: "__ALL__", label: "전체 계좌 (ALL)" }]);
      return;
    }

    fetch("/api/accounts")
      .then((response) => response.json())
      .then((data) => setAccounts(data.accounts || []))
      .catch(() => setError("계좌 정보를 불러오지 못했습니다."));
  }, []);

  // ✅ 계좌/기간/TopK 변경 시 데이터 전체를 다시 조회합니다.
  useEffect(() => {
    const fetchAll = async () => {
      setLoading(true);
      setError(null);

      try {
        if (isMockMode) {
          setKpi(mockData.kpi);
          setSnapshot(mockData.snapshot);
          setTransactions(mockData.transactions.rows);
          setContributions(mockData.contributions.rows);
          setTreemap(mockData.treemap);
          return;
        }

        const params = new URLSearchParams({
          account_id: accountId,
          days: String(days),
        });

        const [kpiRes, snapshotRes, transactionRes, contributionRes, treemapRes] = await Promise.all([
          fetch(`/api/kpi?${params}`),
          fetch(`/api/latest-snapshot?account_id=${accountId}`),
          fetch(`/api/transactions?${params}`),
          fetch(`/api/top-contributions?${params}&top_k=${topK}`),
          fetch(`/api/treemap?${params}`),
        ]);

        const kpiData = await kpiRes.json();
        const snapshotData = await snapshotRes.json();
        const transactionData = await transactionRes.json();
        const contributionData = await contributionRes.json();
        const treemapData = await treemapRes.json();

        setKpi(kpiData.kpi);
        setSnapshot(snapshotData);
        setTransactions(transactionData.rows || []);
        setContributions(contributionData.rows || []);
        setTreemap(treemapData);
      } catch (err) {
        setError("데이터를 불러오는 중 문제가 발생했습니다.");
      } finally {
        setLoading(false);
      }
    };

    fetchAll();
  }, [accountId, days, topK]);

  // ✅ Treemap 데이터가 바뀔 때 Plotly 차트를 다시 그립니다.
  useEffect(() => {
    if (!treemap?.rows?.length) {
      return;
    }

    // 초보자 설명:
    // - Plotly treemap은 labels/parents/values 배열을 요구합니다.
    // - 계층 구조를 만들기 위해 Market -> Asset Type -> Asset Name 순서로 노드를 생성합니다.
    const labels = [];
    const parents = [];
    const values = [];
    const ids = [];

    const marketMap = new Map();
    const typeMap = new Map();

    treemap.rows.forEach((row) => {
      const marketId = `market:${row.market || "기타"}`;
      const typeId = `type:${row.market || "기타"}:${row.asset_type || "기타"}`;
      const assetId = `asset:${row.asset_id}`;

      if (!marketMap.has(marketId)) {
        marketMap.set(marketId, { label: row.market || "기타", value: 0 });
      }
      if (!typeMap.has(typeId)) {
        typeMap.set(typeId, { label: row.asset_type || "기타", value: 0, parent: marketId });
      }

      marketMap.get(marketId).value += row.value;
      typeMap.get(typeId).value += row.value;

      ids.push(assetId);
      labels.push(row.name_kr || String(row.asset_id));
      parents.push(typeId);
      values.push(row.value);
    });

    marketMap.forEach((market, marketId) => {
      ids.push(marketId);
      labels.push(market.label);
      parents.push("");
      values.push(market.value);
    });

    typeMap.forEach((assetType, typeId) => {
      ids.push(typeId);
      labels.push(assetType.label);
      parents.push(assetType.parent);
      values.push(assetType.value);
    });

    const data = [
      {
        type: "treemap",
        ids,
        labels,
        parents,
        values,
        textinfo: "label+value",
        hovertemplate: "%{label}<br>평가금액: %{value:,.0f}<extra></extra>",
      },
    ];

    Plotly.newPlot("treemap", data, {
      margin: { t: 10, l: 10, r: 10, b: 10 },
      height: 420,
    });
  }, [treemap]);

  const kpiCards = useMemo(() => {
    if (!kpi) {
      return [];
    }

    return [
      { label: "평가금액", value: `${formatCurrency(kpi.total_valuation)} 원` },
      { label: "투자원금", value: `${formatCurrency(kpi.total_purchase)} 원` },
      { label: "평가손익", value: `${formatCurrency(kpi.profit)} 원` },
      { label: "누적 수익률", value: `${formatPercent(kpi.portfolio_return_pct)}%` },
    ];
  }, [kpi]);

  return (
    <div className="app">
      <div className="header">
        <button
          className="toggle-button"
          onClick={() => {
            window.location.href = "/?force_desktop=1";
          }}
          type="button"
        >
          📊
        </button>
        <h1>📱 모바일 포트폴리오 요약</h1>
        <p className="badge">모바일 전용 요약 화면</p>
      </div>

      <div className="controls">
        <div>
          <label>계좌 선택</label>
          <select value={accountId} onChange={(event) => setAccountId(event.target.value)}>
            {accounts.map((acc) => (
              <option key={acc.id} value={acc.id}>
                {acc.label}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label>최근 n일</label>
          <input
            type="number"
            min="1"
            value={days}
            onChange={(event) => setDays(Number(event.target.value))}
          />
        </div>
        <div>
          <label>Top K</label>
          <input
            type="number"
            min="1"
            value={topK}
            onChange={(event) => setTopK(Number(event.target.value))}
          />
        </div>
      </div>

      {error && <div className="notice">{error}</div>}
      {loading && <div className="loading">데이터를 불러오는 중입니다...</div>}

      <section className="section">
        <h2>전체 포트폴리오 KPI</h2>
        <div className="card-grid">
          {kpiCards.map((card) => (
            <div className="card" key={card.label}>
              <div className="label">{card.label}</div>
              <div className="value">{card.value}</div>
            </div>
          ))}
        </div>
      </section>

      <section className="section">
        <h2>가장 마지막 날의 스냅샷</h2>
        {snapshot?.latest_date && (
          <p className="badge">기준일: {snapshot.latest_date}</p>
        )}
        <table className="table">
          <thead>
            <tr>
              <th>계좌</th>
              <th>자산명</th>
              <th>수량</th>
              <th>평가금액</th>
              <th>수익률</th>
            </tr>
          </thead>
          <tbody>
            {(snapshot?.rows || []).map((row, index) => (
              <tr key={`${row.asset_name}-${index}`}>
                <td>{row.account_name || "-"}</td>
                <td>{row.asset_name || "-"}</td>
                <td>{formatNumber(row.quantity)}</td>
                <td>{formatCurrency(row.valuation_amount)}</td>
                <td>{formatPercent(row.profit_rate)}%</td>
              </tr>
            ))}
            {(snapshot?.rows || []).length === 0 && (
              <tr>
                <td colSpan="5">표시할 데이터가 없습니다.</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>

      <section className="section">
        <h2>최근 {days}일 거래 내역</h2>
        <table className="table">
          <thead>
            <tr>
              <th>거래일</th>
              <th>구분</th>
              <th>종목</th>
              <th>수량</th>
              <th>단가</th>
            </tr>
          </thead>
          <tbody>
            {transactions.map((row, index) => (
              <tr key={`${row.transaction_date}-${index}`}>
                <td>{row.transaction_date}</td>
                <td>{row.trade_type}</td>
                <td>{row.asset_name || row.ticker || "-"}</td>
                <td>{formatNumber(row.quantity)}</td>
                <td>{formatCurrency(row.price)}</td>
              </tr>
            ))}
            {transactions.length === 0 && (
              <tr>
                <td colSpan="5">최근 거래 내역이 없습니다.</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>

      <section className="section">
        <h2>최근 {days}일 수익률 기여 Top {topK}</h2>
        <table className="table">
          <thead>
            <tr>
              <th>종목</th>
              <th>누적 기여도(%)</th>
            </tr>
          </thead>
          <tbody>
            {contributions.map((row) => (
              <tr key={row.asset_id}>
                <td>{row.name_kr}</td>
                <td>{formatPercent(row.cum_contribution_pct)}%</td>
              </tr>
            ))}
            {contributions.length === 0 && (
              <tr>
                <td colSpan="2">기여도 데이터가 없습니다.</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>

      <section className="section">
        <h2>전체 포트폴리오 Treemap</h2>
        {treemap?.latest_date && (
          <p className="badge">기준일: {treemap.latest_date}</p>
        )}
        <div id="treemap" className="treemap"></div>
      </section>
    </div>
  );
};

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
