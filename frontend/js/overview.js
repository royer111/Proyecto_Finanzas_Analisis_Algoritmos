// ============================================================
// overview.js — Panel de Resumen
// ============================================================

import { RISK_COLORS } from './config.js';
import { S }           from './state.js';
import { destroyChart } from './utils.js';

/**
 * Rellena los stat-chips y construye los tres graficos del panel Resumen.
 */
export function buildOverview() {
    const rd = S.riskData;
    if (!rd || !rd.length) return;

    const avg = (rd.reduce((s, a) => s + a.volatility, 0) / rd.length).toFixed(1);

    document.getElementById('st-assets').textContent = rd.length;
    document.getElementById('st-cons').textContent   = `${rd[0].ticker} ${rd[0].volatility}%`;
    document.getElementById('st-agg').textContent    = `${rd[rd.length - 1].ticker} ${rd[rd.length - 1].volatility}%`;
    document.getElementById('st-avg').textContent    = `${avg}%`;

    buildOvRiskChart(rd);
    buildOvDonut(rd);
    if (S.patternsData) buildOvPatterns(S.patternsData);
}

// ── Barras horizontales de volatilidad por activo ─────────────────────────
function buildOvRiskChart(rd) {
    destroyChart('ov-risk');
    const ctx = document.getElementById('ch-ov-risk').getContext('2d');
    S.charts['ov-risk'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: rd.map(a => a.ticker),
            datasets: [{
                data:            rd.map(a => a.volatility),
                backgroundColor: rd.map(a => RISK_COLORS[a.category] + '88'),
                borderColor:     rd.map(a => RISK_COLORS[a.category]),
                borderWidth: 1, borderRadius: 4,
            }],
        },
        options: {
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: '#1a2d44' }, ticks: { callback: v => v + '%' } },
                y: { grid: { display: false },   ticks: { font: { family: 'Space Mono', size: 9 } } },
            },
        },
    });
}

// ── Dona de distribucion por categoria ────────────────────────────────────
function buildOvDonut(rd) {
    destroyChart('ov-donut');
    const cnt = { conservador: 0, moderado: 0, agresivo: 0 };
    rd.forEach(a => cnt[a.category]++);
    const ctx = document.getElementById('ch-ov-donut').getContext('2d');
    S.charts['ov-donut'] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Conservador', 'Moderado', 'Agresivo'],
            datasets: [{
                data:            [cnt.conservador, cnt.moderado, cnt.agresivo],
                backgroundColor: ['#00e67633', '#ffab4033', '#ff444433'],
                borderColor:     ['#00e676',   '#ffab40',   '#ff4444'],
                borderWidth: 2, hoverOffset: 8,
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { position: 'bottom', labels: { padding: 16 } },
                tooltip: { callbacks: { label: c => `${c.label}: ${c.raw} activos` } },
            },
            cutout: '65%',
        },
    });
}

// ── Barras agrupadas de frecuencia de patrones ────────────────────────────
function buildOvPatterns(pd) {
    destroyChart('ov-patterns');
    const up = pd.filter(p => p.pattern.includes('consecutive')).sort((a, b) => a.ticker.localeCompare(b.ticker));
    const sq = pd.filter(p => p.pattern.includes('squeeze')).sort((a, b) => a.ticker.localeCompare(b.ticker));
    const ctx = document.getElementById('ch-ov-patterns').getContext('2d');
    S.charts['ov-patterns'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: up.map(p => p.ticker),
            datasets: [
                { label: 'Alza Consecutiva', data: up.map(p => +(p.frequency * 100).toFixed(2)), backgroundColor: '#00d4ff33', borderColor: '#00d4ff', borderWidth: 1, borderRadius: 3 },
                { label: 'Vol. Squeeze',     data: sq.map(p => +(p.frequency * 100).toFixed(2)), backgroundColor: '#9b59b633', borderColor: '#9b59b6', borderWidth: 1, borderRadius: 3 },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { labels: { font: { family: 'Space Mono', size: 10 } } } },
            scales: {
                x: { grid: { display: false }, ticks: { font: { family: 'Space Mono', size: 9 } } },
                y: { grid: { color: '#1a2d44' }, ticks: { callback: v => v + '%' } },
            },
        },
    });
}