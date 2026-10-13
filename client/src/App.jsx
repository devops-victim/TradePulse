import { useState, useEffect, useRef } from 'react';
import { useTradePulseSSE, jobStatus } from './hooks/useTradePulseSSE';
import './App.css';

export default function App() {
  const {
    market,
    trades,
    pagination,
    stats,
    importStatus,
    isConnected,
    orders,
    error,
    isLoading,
    setImportStatus,
    refreshSource,
    retryPull,
    fetchPage,
    resetDatabase
  } = useTradePulseSSE();

  // Import configuration
  const [importDelaySeconds, setImportDelaySeconds] = useState(900);
  const [importSymbol, setImportSymbol] = useState('');
  const submissionKey = useRef(null);
  const [isCheckingSource, setIsCheckingSource] = useState(false);
  const [importMode, setImportMode] = useState('append'); // 'append' or 'replace'
  const [isStartingImport, setIsStartingImport] = useState(false);
  const [elapsedWait, setElapsedWait] = useState(0);
  const [isResetting, setIsResetting] = useState(false);

  // Manual order-entry state
  const [selectedSymbol, setSelectedSymbol] = useState('');
  const [tradeType, setTradeType] = useState('BUY');
  const [quantity, setQuantity] = useState('100');
  const [priceInput, setPriceInput] = useState('');
  const [clientName, setClientName] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [feedback, setFeedback] = useState(null);

  useEffect(() => {
    if (importStatus.state !== 'PREPARING') return;
    const timer = setInterval(() => setElapsedWait(Math.floor((Date.now() - (importStatus.startedAt || Date.now())) / 1000)), 1000);
    return () => clearInterval(timer);
  }, [importStatus.state, importStatus.startedAt]);

  const effectiveSymbol = market.some(item => item.symbol === selectedSymbol) ? selectedSymbol : market[0]?.symbol || '';
  const currentAsset = market.find((m) => m.symbol === effectiveSymbol);
  const activePrice = priceInput ? parseFloat(priceInput) : currentAsset?.price || 0;
  const duration = (seconds) => `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  const estimatedTotal = (parseFloat(quantity || '0') * activePrice).toFixed(2);

  const handleStartImport = async () => {
    setElapsedWait(0);
    setIsStartingImport(true);
    setFeedback(null);
    try {
      const delayMs = parseInt(importDelaySeconds, 10) * 1000;
      const signature = JSON.stringify({delayMs, symbol:importSymbol, mode:importMode});
      if (submissionKey.current?.signature !== signature) submissionKey.current = {signature,key:crypto.randomUUID()};
      const res = await fetch('/api/pulls', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(20000),
        body: JSON.stringify({
          delayMs,
          ...(importSymbol ? {symbol:importSymbol} : {}),
          mode: importMode,
          idempotencyKey: submissionKey.current.key
        }),
      });

      const data = await res.json();
      if (!res.ok) {
        setFeedback({
          type: 'error',
          message: data.error || 'Failed to start import'
        });
      } else {
        setImportStatus(previous => previous.jobId===data.job.jobId && previous.updatedAt && new Date(previous.updatedAt)>new Date(data.job.updatedAt) ? previous : jobStatus(data.job));
        submissionKey.current = null;
        setFeedback({
          type: 'info',
          message: data.message || `Import queued! Job ID: ${data.jobId}`
        });
      }
    } catch (err) {
      setFeedback({ type: 'error', message: err.message });
    } finally {
      setIsStartingImport(false);
    }
  };

  const handleResetDb = async () => {
    if (!window.confirm('Delete all trades? This cannot be undone.')) return;
    setIsResetting(true);
    try {
      const result = await resetDatabase();
      if (!result.success) throw new Error(result.error || "Unable to clear trades");
      setFeedback({ type: 'info', message: 'All trades cleared.' });
    } catch (err) {
      setFeedback({ type: 'error', message: 'Failed to reset database: ' + err.message });
    } finally {
      setIsResetting(false);
    }
  };

  const handleTradeSubmit = async (e) => {
    e.preventDefault();
    if (!quantity || parseFloat(quantity) <= 0) return;

    setIsSubmitting(true);
    setFeedback(null);

    try {
      const res = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(20000),
        body: JSON.stringify({
          symbol: effectiveSymbol,
          client: clientName,
          type: tradeType,
          quantity: Number(quantity),
          price: priceInput || String(currentAsset?.price || '0'),
        }),
      });

      const data = await res.json();
      if (data.success) {
        setFeedback({
          type: 'success',
          message: `Manual order saved: ${tradeType} ${quantity} ${effectiveSymbol}.`,
        });
        setTimeout(() => setFeedback(null), 4000);
      } else {
        setFeedback({ type: 'error', message: data.error || 'Failed to place order' });
      }
    } catch (err) {
      setFeedback({ type: 'error', message: err.message });
    } finally {
      setIsSubmitting(false);
    }
  };

  const isImportActive = importStatus.state === 'PREPARING' || importStatus.state === 'STAGING';

  const money = (value) => Number(value || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const routes = [
    ['/api/trades?page=1&limit=30', 'Trades'],
    ['/api/trades/stats', 'Trade totals'],
    ['/api/market', 'Market prices'],
    ['/api/pulls', 'Import jobs'],
    ['/api/health', 'System status'],
    ['/api/orders', 'Manual orders'],
  ];

  return (
    <div className="app">
      <header className="header">
        <a className="brand" href="#trades">TradePulse</a>
        <nav aria-label="Main navigation">
          <a href="#trades">Trades</a><a href="#import">Import</a><a href="#order">New order</a><a href="#routes">API routes</a>
        </nav>
        <span className={`connection ${isConnected ? 'online' : ''}`}>{isConnected ? 'Live updates connected' : 'Connecting to live updates...'}</span>
      </header>
      <main>
        <div className="page-heading"><div><h1>Trade dashboard</h1><p>Persisted BSE trades and latest recorded prices.</p></div><button onClick={() => fetchPage(pagination.page)}>Refresh trades</button></div>
        {error && <div className="notice error" role="alert">{error} <button onClick={() => fetchPage(pagination.page)}>Retry</button></div>}
        {feedback && <div className={`notice ${feedback.type}`} role={feedback.type === 'error' ? 'alert' : 'status'}>{feedback.message}</div>}
        <dl className="summary">
          <div><dt>Total stored trades (30 shown per page)</dt><dd>{stats.totalTrades.toLocaleString('en-IN')}</dd></div>
          <div><dt>Trade value (INR)</dt><dd>INR {money(stats.totalVolume)}</dd></div>
          <div><dt>Buy trades</dt><dd>{stats.buyTrades?.toLocaleString('en-IN')}</dd></div>
          <div><dt>Sell trades</dt><dd>{stats.sellTrades?.toLocaleString('en-IN')}</dd></div>
        </dl>
        <section id="trades" className="panel">
          <div className="section-heading"><div><h2>Trades</h2><p>Newest first | 30 rows per page</p></div><a className="route" href={`/api/trades?page=${pagination.page}&limit=30`} target="_blank" rel="noreferrer">GET /api/trades (JSON)</a></div>
          {isLoading && <p role="status">Loading trades...</p>}
          {isLoading && trades.length === 0 ? null : trades.length === 0 ? <div className="empty"><h3>No trades to show</h3><p>Start an external BSE pull to populate the ledger.</p><a href="#import">Go to import</a></div> : <>
            <div className="table-scroll"><table><caption className="sr-only">Trade ledger, page {pagination.page}</caption><thead><tr><th scope="col">Trade ID</th><th scope="col">Client</th><th scope="col">Symbol</th><th scope="col">Side</th><th scope="col" className="number">Quantity</th><th scope="col" className="number">Price (INR)</th><th scope="col" className="number">Value (INR)</th><th scope="col">Executed at</th></tr></thead><tbody>
              {trades.map(t => <tr key={t.id || t.trade_id}><td className="mono"><a href={`/api/trades/${encodeURIComponent(t.trade_id)}`} target="_blank" rel="noreferrer">{t.trade_id}</a></td><td>{t.client || 'Institutional Desk'}</td><td className="symbol">{t.symbol}</td><td className={t.type === 'BUY' ? 'buy' : t.type === 'SELL' ? 'sell' : ''}>{t.type || '-'}</td><td className="number">{Number(t.quantity).toLocaleString('en-IN')}</td><td className="number">{money(t.price)}</td><td className="number">{money(t.total ?? t.quantity * t.price)}</td><td>{t.created_at ? new Date(t.created_at).toLocaleString('en-IN') : '-'}</td></tr>)}
            </tbody></table></div>
            <div className="pagination"><span>Showing {(pagination.page - 1) * 30 + 1} to {(pagination.page - 1) * 30 + trades.length} of {pagination.total?.toLocaleString('en-IN')}</span><div><button onClick={() => fetchPage(1)} disabled={!pagination.hasPrevPage}>First</button><button onClick={() => fetchPage(pagination.page - 1)} disabled={!pagination.hasPrevPage}>Previous</button><span>Page {pagination.page} of {pagination.totalPages}</span><button onClick={() => fetchPage(pagination.page + 1)} disabled={!pagination.hasNextPage}>Next</button><button onClick={() => fetchPage(pagination.totalPages)} disabled={!pagination.hasNextPage}>Last</button></div></div>
          </>}
        </section>
        <section id="import" className="panel">
          <div className="section-heading"><div><h2>Import trades</h2><p>The import is accepted immediately. Extraction runs at the external BSE service; existing trades remain available.</p></div><code className="route">POST /api/pulls</code></div>
          <div className="form-grid import-grid">
            <label>Delay<select value={importDelaySeconds} onChange={e => setImportDelaySeconds(Number(e.target.value))} disabled={isImportActive}>{[0,5,15,30,60,300,720,900].map(n => <option key={n} value={n}>{n === 0 ? 'No delay' : n >= 60 ? `${n / 60} minutes` : `${n} seconds`}</option>)}</select></label>
            <label>Symbol<select value={importSymbol} onChange={e => setImportSymbol(e.target.value)} disabled={isImportActive}><option value="">All symbols</option>{market.map(item => <option key={item.symbol} value={item.symbol}>{item.symbol}</option>)}</select></label>
            <label>Mode<select value={importMode} onChange={e => setImportMode(e.target.value)} disabled={isImportActive}><option value="append">Append trades</option><option value="replace">Replace all trades</option></select></label>
            <button className="primary" onClick={handleStartImport} disabled={isStartingImport || isImportActive}>{isStartingImport ? 'Starting...' : isImportActive ? 'Import in progress...' : 'Start import'}</button>
          </div>
          {importStatus.completionMode === 'manual' && <p className="import-status">The external service does not yet send completion notifications. Use Check source status to request its status once; automatic arrival requires source notifications.</p>}
          {importStatus.jobId && <p className="import-status">Job: <a className="route" href={`/api/pulls/${importStatus.jobId}`} target="_blank" rel="noreferrer">{importStatus.jobId}</a>{importStatus.externalJobId && <span> | External extraction: {importStatus.externalJobId}</span>}</p>}
          {importStatus.jobId && <div className="job-actions">{isImportActive && <button disabled={isCheckingSource} onClick={async () => {setIsCheckingSource(true);try {const result = await refreshSource(importStatus.jobId);setFeedback({type:'info',message:`External status: ${result.sourceStatus}`});} catch(error) {setFeedback({type:'error',message:error.message});} finally {setIsCheckingSource(false);}}}>{isCheckingSource ? 'Checking...' : 'Check source status'}</button>}{importStatus.state === 'FAILED' && <button onClick={async () => {try {await retryPull(importStatus.jobId);} catch(error) {setFeedback({type:'error',message:error.message});}}}>Retry ingestion</button>}</div>}
          <p className="import-status">Requests have a 20-second deadline. Live connections rotate every 25 seconds and reconnect; the background import continues.</p>
          <p className={`import-status ${importStatus.state === 'FAILED' ? 'sell' : ''}`} role="status"><strong>Status: {importStatus.state.toLowerCase()}</strong>{importStatus.state === 'PREPARING' ? ` | ${duration(elapsedWait)} elapsed / ${duration(importStatus.delayMs / 1000)} source delay` : importStatus.message ? ` | ${importStatus.message}` : ''}</p>
        </section>
        <section id="order" className="panel">
          <div className="section-heading"><div><h2>New order</h2><p>Manual orders are recorded separately from externally ingested trades.</p></div><code className="route">POST /api/orders</code></div>
          <form onSubmit={handleTradeSubmit}>
            <div className="form-grid order-grid">
              <label>Symbol<select value={effectiveSymbol} onChange={e => {setSelectedSymbol(e.target.value); setPriceInput('');}} disabled={!market.length}>{!market.length && <option value="">No recorded prices</option>}{market.map(m => <option key={m.symbol} value={m.symbol}>{m.symbol}</option>)}</select></label>
              <label>Client<input required value={clientName} onChange={e => setClientName(e.target.value)} /></label>
              <label>Side<select value={tradeType} onChange={e => setTradeType(e.target.value)}><option>BUY</option><option>SELL</option></select></label>
              <label>Quantity<input required type="number" min="1" step="1" value={quantity} onChange={e => setQuantity(e.target.value)} /></label>
              <label>Price (INR)<input required type="number" min="0.01" step="0.01" value={priceInput || activePrice} onChange={e => setPriceInput(e.target.value)} /></label>
            </div>
            <div className="form-footer"><span>Estimated value: <strong>INR {money(estimatedTotal)}</strong></span><button className="primary" type="submit" disabled={isSubmitting || !market.length}>{isSubmitting ? 'Saving...' : 'Save order'}</button></div>
          </form>
          {orders.length > 0 && <div className="table-scroll"><table><caption>Recent manual orders</caption><thead><tr><th>Client</th><th>Symbol</th><th>Side</th><th className="number">Quantity</th><th className="number">Price (INR)</th></tr></thead><tbody>{orders.map(order => <tr key={order.order_id}><td>{order.client}</td><td>{order.symbol}</td><td>{order.side}</td><td className="number">{order.quantity}</td><td className="number">{money(order.price)}</td></tr>)}</tbody></table></div>}
        </section>
        <section className="panel"><div className="section-heading"><h2>Market prices</h2><a className="route" href="/api/market" target="_blank" rel="noreferrer">GET /api/market (JSON)</a></div><div className="market-list">{market.length ? market.map(m => <div key={m.symbol}><span>{m.symbol}</span><strong>INR {money(m.price)}</strong></div>) : <p>No recorded market prices yet.</p>}</div></section>
        <section id="routes" className="panel"><div className="section-heading"><div><h2>API routes</h2><p>Open a GET route to view its JSON response. POST routes are called by the forms above.</p></div></div><div className="route-list">{routes.map(([url,label]) => <a href={url} key={url} target="_blank" rel="noreferrer"><span>{label}</span><code>GET {url} (JSON)</code></a>)}</div><div className="reset-row"><div><strong>Clear all trades</strong><p><code>POST /api/reset</code> | Permanently deletes the ledger.</p></div><button className="danger" onClick={handleResetDb} disabled={isResetting || isImportActive}>{isResetting ? 'Clearing...' : 'Clear trades'}</button></div></section>
      </main>
      <footer>TradePulse | External BSE source | All amounts in INR</footer>
    </div>
  );
}
