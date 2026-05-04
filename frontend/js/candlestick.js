// ============================================================
// candlestick.js — Panel de Candlestick (Canvas API nativo)
// ============================================================

import { S }            from './state.js';
import { apiFetch, toast } from './utils.js';

let _inited = false;

// ── Selector de tickers ───────────────────────────────────────────────────
export function buildTickerSelector() {
    const tickers = S.tickers.length
        ? S.tickers
        : ['NVDA', 'TSLA', 'VOO', 'GLD', 'AAPL', 'MSFT', 'META', 'AMZN', 'QQQ', 'XLK'];

    const el = document.getElementById('ticker-sel');
    el.innerHTML = tickers.map((t, i) =>
        `<button class="ticker-btn${i === 0 ? ' active' : ''}" data-ticker="${t}">${t}</button>`
    ).join('');
    S.candleTicker = tickers[0];
}

export function selectTicker(ticker, btn) {
    document.querySelectorAll('.ticker-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    S.candleTicker = ticker;
    reloadCandle();
}

// ── Inicio lazy del panel ─────────────────────────────────────────────────
export function initCandlestick() {
    if (_inited) return;
    _inited = true;
    reloadCandle();
}

// ── Recarga de datos desde la API ─────────────────────────────────────────
export async function reloadCandle() {
    if (!S.candleTicker) return;
    const period = parseInt(document.getElementById('sel-period').value) || 0;
    const url = `/dashboard/candlestick/${S.candleTicker}?sma_windows=20,50,200${period ? `&last_n=${period}` : ''}`;
    try {
        S.candleData = await apiFetch(url);
        drawCandle();
    } catch (e) {
        console.warn('Candlestick API failed, using simulation:', e.message);
        S.candleData = simulateCandle(S.candleTicker);
        drawCandle();
        toast(`Datos simulados para ${S.candleTicker}`, '');
    }
}

// ── Generador de datos simulados (fallback) ───────────────────────────────
function simulateCandle(ticker) {
    const startPrices = { NVDA: 400, TSLA: 200, VOO: 400, GLD: 180, AAPL: 150, MSFT: 300, META: 250, AMZN: 130, QQQ: 350, XLK: 160 };
    let price = startPrices[ticker] || 200;
    const dates = [], opens = [], highs = [], lows = [], closes = [], volumes = [];
    const today = new Date();

    for (let i = 252; i >= 0; i--) {
        const d = new Date(today);
        d.setDate(d.getDate() - i);
        if (d.getDay() === 0 || d.getDay() === 6) continue;
        const chg = (Math.random() - 0.48) * price * 0.025;
        const o = price, c = Math.max(1, price + chg);
        dates.push(d.toISOString().slice(0, 10));
        opens.push(+o.toFixed(2));
        closes.push(+c.toFixed(2));
        highs.push(+(Math.max(o, c) * (1 + Math.random() * 0.01)).toFixed(2));
        lows.push(+(Math.min(o, c) * (1 - Math.random() * 0.01)).toFixed(2));
        volumes.push(Math.round(1e6 + Math.random() * 5e6));
        price = c;
    }

    const sma20 = [], sma50 = [], sma200 = [];
    for (let i = 0; i < closes.length; i++) {
        sma20.push(i  >= 19  ? closes.slice(i - 19,  i + 1).reduce((a, b) => a + b, 0) / 20  : null);
        sma50.push(i  >= 49  ? closes.slice(i - 49,  i + 1).reduce((a, b) => a + b, 0) / 50  : null);
        sma200.push(i >= 199 ? closes.slice(i - 199, i + 1).reduce((a, b) => a + b, 0) / 200 : null);
    }
    return { ticker, dates, opens, highs, lows, closes, volumes, sma: { sma20, sma50, sma200 } };
}

// ── Dispatcher principal ──────────────────────────────────────────────────
export function drawCandle() {
    const d = S.candleData;
    if (!d) return;
    const show20  = document.getElementById('cb-sma20')?.checked;
    const show50  = document.getElementById('cb-sma50')?.checked;
    const show200 = document.getElementById('cb-sma200')?.checked;
    drawPriceCanvas(d, show20, show50, show200);
    drawVolumeCanvas(d);
}

// ── Canvas de precio + SMAs ───────────────────────────────────────────────
function drawPriceCanvas(d, show20, show50, show200) {
    const canvas = document.getElementById('cv-price');
    const dpr = window.devicePixelRatio || 1;
    const W   = canvas.parentElement.clientWidth;
    const H   = canvas.parentElement.clientHeight;
    canvas.width  = W * dpr; canvas.height = H * dpr;
    canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);

    const pad = { top: 20, right: 65, bottom: 30, left: 10 };
    const pw = W - pad.left - pad.right;
    const ph = H - pad.top  - pad.bottom;
    const n  = d.dates.length;
    if (!n) return;

    // Rango de precios
    const allPrices = [...d.highs, ...d.lows].filter(v => v != null);
    let minP = Math.min(...allPrices);
    let maxP = Math.max(...allPrices);
    const smaVals = [];
    if (show20)  smaVals.push(...(d.sma.sma20  || []).filter(v => v != null));
    if (show50)  smaVals.push(...(d.sma.sma50  || []).filter(v => v != null));
    if (show200) smaVals.push(...(d.sma.sma200 || []).filter(v => v != null));
    if (smaVals.length) { minP = Math.min(minP, ...smaVals); maxP = Math.max(maxP, ...smaVals); }
    const margin = (maxP - minP) * 0.05;
    minP -= margin; maxP += margin;
    const rangeP = maxP - minP;

    const toY = v  => pad.top + ph * (1 - (v - minP) / rangeP);
    const toX = i  => pad.left + (i + 0.5) * (pw / n);
    const cW  = Math.max(1, pw / n * 0.7);

    // Fondo
    ctx.fillStyle = '#111d2e';
    ctx.fillRect(0, 0, W, H);

    // Grid horizontal
    ctx.strokeStyle = '#1a2d44'; ctx.lineWidth = 0.5;
    for (let i = 0; i <= 5; i++) {
        const y = pad.top + (ph / 5) * i;
        ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
        const val = maxP - (rangeP / 5) * i;
        ctx.fillStyle = '#768d9e'; ctx.font = '10px Space Mono'; ctx.textAlign = 'left';
        ctx.fillText('$' + val.toFixed(0), W - pad.right + 6, y + 4);
    }

    // Velas
    for (let i = 0; i < n; i++) {
        const o = d.opens[i], h = d.highs[i], l = d.lows[i], c = d.closes[i];
        if (o == null || h == null || l == null || c == null) continue;
        const x   = toX(i);
        const col = c >= o ? '#00e676' : '#ff4444';
        ctx.strokeStyle = col; ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(x, toY(h)); ctx.lineTo(x, toY(l)); ctx.stroke();
        const bodyTop = toY(Math.max(o, c));
        const bodyH   = Math.max(1, Math.abs(toY(o) - toY(c)));
        ctx.fillStyle   = c >= o ? '#00e67633' : '#ff444433';
        ctx.strokeStyle = col; ctx.lineWidth = 1;
        ctx.fillRect(x - cW / 2, bodyTop, cW, bodyH);
        ctx.strokeRect(x - cW / 2, bodyTop, cW, bodyH);
    }

    // SMAs
    [
        { key: 'sma20',  color: '#f1c40f', show: show20  },
        { key: 'sma50',  color: '#3498db', show: show50  },
        { key: 'sma200', color: '#e67e22', show: show200 },
    ].forEach(({ key, color, show }) => {
        if (!show || !d.sma[key]) return;
        ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.beginPath();
        let started = false;
        d.sma[key].forEach((v, i) => {
            if (v == null) return;
            const x = toX(i), y = toY(v);
            if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
        });
        ctx.stroke();
    });

    // Eje X: fechas
    ctx.fillStyle = '#768d9e'; ctx.font = '9px Space Mono'; ctx.textAlign = 'center';
    const step = Math.max(1, Math.floor(n / 8));
    for (let i = 0; i < n; i += step) {
        ctx.fillText(d.dates[i]?.slice(0, 7) || '', toX(i), H - 8);
    }

    // Etiqueta del ticker
    ctx.fillStyle = '#cdd9e5'; ctx.font = 'bold 13px DM Sans'; ctx.textAlign = 'left';
    ctx.fillText(d.ticker, pad.left + 8, pad.top + 16);
}

// ── Canvas de volumen ─────────────────────────────────────────────────────
function drawVolumeCanvas(d) {
    const canvas = document.getElementById('cv-volume');
    const dpr = window.devicePixelRatio || 1;
    const W   = canvas.parentElement.clientWidth;
    const H   = canvas.parentElement.clientHeight;
    canvas.width = W * dpr; canvas.height = H * dpr;
    canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);

    const pad = { top: 8, right: 65, bottom: 20, left: 10 };
    const pw = W - pad.left - pad.right;
    const ph = H - pad.top  - pad.bottom;
    const n  = d.dates.length;

    ctx.fillStyle = '#111d2e'; ctx.fillRect(0, 0, W, H);

    const maxVol = Math.max(...d.volumes.filter(v => v != null));
    const barW   = Math.max(1, pw / n * 0.7);
    const toX    = i => pad.left + (i + 0.5) * (pw / n);

    for (let i = 0; i < n; i++) {
        const v = d.volumes[i]; if (!v) continue;
        const o = d.opens[i], c = d.closes[i];
        const bH = (v / maxVol) * ph;
        ctx.fillStyle = (c >= o) ? '#00e67633' : '#ff444433';
        ctx.fillRect(toX(i) - barW / 2, pad.top + ph - bH, barW, bH);
    }

    ctx.fillStyle = '#3d5166'; ctx.font = '9px Space Mono'; ctx.textAlign = 'left';
    ctx.fillText('VOL', W - pad.right + 6, pad.top + 10);
}