// ============================================================
// similarity.js — Panel de Similitud
// ============================================================

import { API }              from './config.js';
import { S }                from './state.js';
import { toast, destroyChart } from './utils.js';

let _simType  = 'prices';
let _inited   = false;

// ── Toggle Precios / Retornos ─────────────────────────────────────────────
export function setSimType(type) {
    _simType = type;
    document.getElementById('btn-prices').classList.toggle('active',  type === 'prices');
    document.getElementById('btn-returns').classList.toggle('active', type === 'returns');
}

// ── Inicio lazy ───────────────────────────────────────────────────────────
export function initSimilarity() {
    if (_inited) return;
    _inited = true;
    _buildSimSelects();
}

// ── Poblar los dos <select> con los tickers del state ─────────────────────
function _buildSimSelects() {
    const tickers = S.tickers.length
        ? S.tickers
        : ['VOO', 'CSPX.L', 'QQQ', 'VTI', 'EFA', 'IEMG', 'GLD', 'TLT',
            'XLF', 'XLK', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA'];

    const opts = tickers.map(t => `<option value="${t}">${t}</option>`).join('');
    document.getElementById('sim-asset-a').innerHTML = opts;
    document.getElementById('sim-asset-b').innerHTML = opts;
    document.getElementById('sim-asset-a').value = tickers[0];
    document.getElementById('sim-asset-b').value = tickers.length > 1 ? tickers[1] : tickers[0];
}

// ── Calculo de similitud ──────────────────────────────────────────────────
export async function calcSimilarity() {
    const assetA = document.getElementById('sim-asset-a').value;
    const assetB = document.getElementById('sim-asset-b').value;

    if (assetA === assetB) { toast('Selecciona dos activos diferentes', 'error'); return; }

    const btn = document.getElementById('btn-calc');
    btn.disabled = true; btn.textContent = 'Calculando...';

    document.getElementById('sim-metrics').style.display    = 'none';
    document.getElementById('sim-csv-info').style.display   = 'none';
    document.getElementById('sim-chart-wrap').style.display = 'none';

    try {
        const res = await fetch(API + '/dashboard/similarity', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ asset_a: assetA, asset_b: assetB, series_type: _simType }),
        });
        if (!res.ok) { const e = await res.json(); throw new Error(e.detail || res.statusText); }
        const data = await res.json();
        _renderSimilarityResults(data);
        toast(`Similitud calculada: ${assetA} vs ${assetB}`, 'success');
    } catch (e) {
        toast('Error: ' + e.message, 'error');
    } finally {
        btn.disabled = false; btn.textContent = 'CALCULAR';
    }
}

// ── Renderizar resultados ─────────────────────────────────────────────────
function _renderSimilarityResults(data) {
    const { asset_a, asset_b, series_type, dates, series_a, series_b,
        metrics, metric_types, csv_path } = data;

    // Tarjetas de metricas
    ['euclidean', 'pearson', 'cosine', 'dtw'].forEach(m => {
        const val  = metrics[m];
        const type = metric_types[m];
        document.getElementById(`mv-${m}`).textContent = val >= 1000 ? val.toFixed(1) : val.toFixed(4);
        document.getElementById(`mc-${m}`).className   = `metric-card ${type}`;
    });
    document.getElementById('sim-metrics').style.display = 'grid';

    // Badge CSV
    const fname = csv_path.split('/').pop().split('\\').pop();
    document.getElementById('sim-csv-name').textContent = fname;
    document.getElementById('sim-csv-info').style.display = 'block';

    // Titulo del grafico
    const label = series_type === 'prices' ? 'Precios' : 'Retornos';
    document.getElementById('sim-chart-title').textContent = `Series de ${label}: ${asset_a} vs ${asset_b}`;
    document.getElementById('sim-chart-wrap').style.display = 'block';

    _buildSimilarityChart(dates, series_a, series_b, asset_a, asset_b, series_type);
}

// ── Grafico de lineas con dos ejes Y ─────────────────────────────────────
function _buildSimilarityChart(dates, seriesA, seriesB, labelA, labelB, seriesType) {
    destroyChart('similarity');

    const maxPoints = 400;
    const step = dates.length > maxPoints ? Math.ceil(dates.length / maxPoints) : 1;
    const fd = dates.filter((_, i)   => i % step === 0);
    const fa = seriesA.filter((_, i) => i % step === 0);
    const fb = seriesB.filter((_, i) => i % step === 0);

    const ctx = document.getElementById('ch-similarity').getContext('2d');
    S.charts['similarity'] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: fd,
            datasets: [
                {
                    label: labelA, data: fa,
                    borderColor: '#00d4ff', backgroundColor: 'rgba(0,212,255,.06)',
                    borderWidth: 1.5, pointRadius: 0, tension: 0, yAxisID: 'yA',
                },
                {
                    label: labelB, data: fb,
                    borderColor: '#ffab40', backgroundColor: 'rgba(255,171,64,.06)',
                    borderWidth: 1.5, pointRadius: 0, tension: 0,
                    yAxisID: seriesType === 'prices' ? 'yB' : 'yA',
                },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { labels: { color: '#cdd9e5', font: { family: 'Space Mono', size: 10 }, padding: 16 } },
                tooltip: {
                    callbacks: {
                        label: c => ` ${c.dataset.label}: ${c.parsed.y >= 100 ? c.parsed.y.toFixed(2) : c.parsed.y.toFixed(4)}`,
                    },
                },
            },
            scales: {
                x: {
                    grid:  { color: '#1a2d44' },
                    ticks: { color: '#768d9e', font: { family: 'Space Mono', size: 9 }, maxTicksLimit: 10 },
                },
                yA: {
                    type: 'linear', position: 'left',
                    grid:  { color: '#1a2d44' },
                    ticks: { color: '#00d4ff', font: { family: 'Space Mono', size: 9 } },
                },
                yB: {
                    type: 'linear', position: 'right',
                    display: seriesType === 'prices',
                    grid:    { drawOnChartArea: false },
                    ticks:   { color: '#ffab40', font: { family: 'Space Mono', size: 9 } },
                },
            },
        },
    });
}