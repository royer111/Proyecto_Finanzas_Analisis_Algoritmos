// ============================================================
// main.js — Punto de entrada de la aplicacion
// ============================================================

import { API } from './config.js';
import { S }                        from './state.js';
import { toast, apiFetch }          from './utils.js';
import { buildOverview }            from './overview.js';
import { buildTickerSelector,
    drawCandle }               from './candlestick.js';

// ── Carga inicial ─────────────────────────────────────────────────────────
async function init() {
    try {
        const [tRes, rRes, pRes] = await Promise.all([
            apiFetch('/dashboard/tickers'),
            apiFetch('/dashboard/risk-ranking'),
            apiFetch('/dashboard/patterns'),
        ]);

        S.tickers      = tRes.tickers || [];
        S.riskData     = (rRes.data && rRes.data.length) ? rRes.data : null;
        S.patternsData = (pRes.data && pRes.data.length) ? pRes.data : null;

        if (!S.riskData)     toast('risk_ranking.csv no encontrado — ejecuta el Req. 3 primero', 'error');
        if (!S.patternsData) toast('patterns_summary.csv no encontrado — ejecuta el Req. 3 primero', 'error');

        buildOverview();
        buildTickerSelector();

    } catch (e) {
        console.error('API error:', e);
        toast('No se pudo conectar con la API en localhost:8000', 'error');
        // loadDemoData();  // descomentar para activar modo demo
    }
}

// ── Modo demo ─────────────────────────────────────────────────────────────
function loadDemoData() {
    S.tickers = ['GLD','TLT','CSPX.L','EFA','VOO','VTI','IEMG','QQQ','XLF','XLK','MSFT','GOOGL','AAPL','AMZN','META','NVDA','TSLA'];
    S.riskData = [
        {position:1,  ticker:'GLD',    volatility:14.95, category:'conservador', risk_score:1},
        {position:2,  ticker:'TLT',    volatility:17.62, category:'conservador', risk_score:1},
        {position:3,  ticker:'CSPX.L', volatility:18.96, category:'conservador', risk_score:1},
        {position:4,  ticker:'EFA',    volatility:20.05, category:'conservador', risk_score:1},
        {position:5,  ticker:'VOO',    volatility:21.34, category:'conservador', risk_score:1},
        {position:6,  ticker:'VTI',    volatility:21.66, category:'moderado',    risk_score:2},
        {position:7,  ticker:'IEMG',   volatility:22.33, category:'moderado',    risk_score:2},
        {position:8,  ticker:'QQQ',    volatility:25.50, category:'moderado',    risk_score:2},
        {position:9,  ticker:'XLF',    volatility:26.66, category:'moderado',    risk_score:2},
        {position:10, ticker:'XLK',    volatility:27.43, category:'moderado',    risk_score:2},
        {position:11, ticker:'MSFT',   volatility:30.53, category:'moderado',    risk_score:2},
        {position:12, ticker:'GOOGL',  volatility:31.84, category:'agresivo',    risk_score:3},
        {position:13, ticker:'AAPL',   volatility:32.26, category:'agresivo',    risk_score:3},
        {position:14, ticker:'AMZN',   volatility:35.21, category:'agresivo',    risk_score:3},
        {position:15, ticker:'META',   volatility:44.26, category:'agresivo',    risk_score:3},
        {position:16, ticker:'NVDA',   volatility:51.51, category:'agresivo',    risk_score:3},
        {position:17, ticker:'TSLA',   volatility:64.72, category:'agresivo',    risk_score:3},
    ];
    const pt = ['AAPL','AMZN','CSPX.L','EFA','GLD','GOOGL','IEMG','META','MSFT','NVDA','QQQ','TLT','TSLA','VOO','VTI','XLF','XLK'];
    const uf = [0.142,0.143,0.166,0.135,0.137,0.150,0.127,0.129,0.142,0.157,0.170,0.110,0.156,0.157,0.158,0.140,0.155];
    const sf = [0.153,0.147,0.202,0.153,0.103,0.128,0.129,0.130,0.138,0.139,0.152,0.100,0.124,0.178,0.179,0.121,0.138];
    S.patternsData = pt.flatMap((t, i) => [
        { ticker: t, pattern: 'consecutive_up',     window: 3, occurrences: Math.round(uf[i]*1257), frequency: uf[i] },
        { ticker: t, pattern: 'volatility_squeeze', window: 5, occurrences: Math.round(sf[i]*1232), frequency: sf[i] },
    ]);
    buildOverview();
    buildTickerSelector();
    toast('Modo demo — datos del Req. 3 precargados', '');
}

// ── Exportacion PDF ───────────────────────────────────────────────────────
export async function exportPDF() {
    const btn = document.getElementById('btn-pdf');
    btn.disabled    = true;
    btn.textContent = '⏳ Generando...';
    toast('Generando reporte PDF...', '');
    try {
        const tickers = S.candleTicker
            ? [S.candleTicker, ...['NVDA','TSLA','VOO','GLD'].filter(t => t !== S.candleTicker)].slice(0, 4)
            : ['NVDA','TSLA','VOO','GLD'];

        const res = await fetch(`${API}/export/pdf`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ tickers }),
        });
        if (!res.ok) throw new Error(await res.text());

        const blob = await res.blob();
        const url  = URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href = url; a.download = 'reporte_tecnico_bursatil.pdf'; a.click();
        URL.revokeObjectURL(url);
        toast('PDF descargado ✓', 'success');
    } catch (e) {
        toast('Error exportando PDF: ' + e.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
      <polyline points="7 10 12 15 17 10"/>
      <line x1="12" y1="15" x2="12" y2="3"/>
    </svg> EXPORTAR PDF`;
    }
}

// ── Resize ────────────────────────────────────────────────────────────────
window.addEventListener('resize', () => { if (S.candleData) drawCandle(); });

// ── Arranque ──────────────────────────────────────────────────────────────
init();