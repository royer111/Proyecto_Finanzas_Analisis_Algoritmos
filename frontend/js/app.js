// ============================================================
// app.js — Unico punto de entrada. Orquesta arranque y eventos.
// ============================================================

import { applyChartDefaults }    from './config.js';
import { switchTab }             from './tabs.js';
import { exportPDF }             from './main.js';
import { drawCandle,
    reloadCandle,
    selectTicker }          from './candlestick.js';
import { setSimType,
    calcSimilarity }        from './similarity.js';

// 1. Chart.js defaults (Chart global ya esta disponible porque
//    el <script src="chart.umd.min.js"> es un script normal, no modulo)
applyChartDefaults();

// 2. Registrar todos los event listeners una vez el DOM esta listo.
//    DOMContentLoaded ya paso cuando los modulos se ejecutan,
//    pero usamos un helper por seguridad.
function ready(fn) {
    if (document.readyState !== 'loading') fn();
    else document.addEventListener('DOMContentLoaded', fn);
}

ready(() => {
    // ── Tabs ──────────────────────────────────────────────────
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const panelId = tab.getAttribute('data-tab');
            if (panelId) switchTab(panelId);
        });
    });

    // ── Boton exportar PDF ─────────────────────────────────────
    const btnPdf = document.getElementById('btn-pdf');
    if (btnPdf) btnPdf.addEventListener('click', exportPDF);

    // ── Candlestick: checkboxes SMA ────────────────────────────
    ['cb-sma20', 'cb-sma50', 'cb-sma200'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('change', drawCandle);
    });

    // ── Candlestick: selector de periodo ──────────────────────
    const selPeriod = document.getElementById('sel-period');
    if (selPeriod) selPeriod.addEventListener('change', reloadCandle);

    // ── Similitud: toggle Precios / Retornos ──────────────────
    const btnPrices  = document.getElementById('btn-prices');
    const btnReturns = document.getElementById('btn-returns');
    if (btnPrices)  btnPrices.addEventListener('click',  () => setSimType('prices'));
    if (btnReturns) btnReturns.addEventListener('click', () => setSimType('returns'));

    // ── Similitud: boton calcular ─────────────────────────────
    const btnCalc = document.getElementById('btn-calc');
    if (btnCalc) btnCalc.addEventListener('click', calcSimilarity);

    // ── Candlestick: selector de ticker (delegacion) ──────────
    // Los botones de ticker se generan dinamicamente, asi que
    // delegamos el evento al contenedor padre
    const tickerSel = document.getElementById('ticker-sel');
    if (tickerSel) {
        tickerSel.addEventListener('click', e => {
            const btn = e.target.closest('.ticker-btn');
            if (btn) selectTicker(btn.dataset.ticker, btn);
        });
    }
});