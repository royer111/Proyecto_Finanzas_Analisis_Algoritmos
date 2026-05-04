// ============================================================
// patterns.js — Panel de Patrones
// ============================================================

import { S }           from './state.js';
import { destroyChart } from './utils.js';

/**
 * Punto de entrada del panel. Usa los datos ya cargados en state.
 */
export function renderPatternsPanel() {
    const pd = S.patternsData;
    if (!pd || !pd.length) {
        const msg = '<div class="loader" style="color:var(--red)">patterns_summary.csv no encontrado.<br>Ejecuta el Req. 3 primero.</div>';
        document.getElementById('pat-up-list').innerHTML = msg;
        document.getElementById('pat-sq-list').innerHTML = msg;
        return;
    }
    renderPatternList('consecutive', 'pat-up-list', '#00d4ff');
    renderPatternList('squeeze',     'pat-sq-list', '#9b59b6');
    buildPatternsCompare(pd);
}

// ── Lista de barras de frecuencia por patron ──────────────────────────────
function renderPatternList(key, containerId, color) {
    const data = S.patternsData.filter(p => p.pattern.includes(key));
    data.sort((a, b) => b.frequency - a.frequency);
    const maxF = Math.max(...data.map(p => p.frequency));
    document.getElementById(containerId).innerHTML = data.map(p => `
    <div class="pattern-row">
      <span class="pattern-ticker">${p.ticker}</span>
      <div class="freq-bar">
        <div class="freq-fill" style="width:${(p.frequency / maxF * 100).toFixed(1)}%;background:${color}44;border-right:2px solid ${color}"></div>
      </div>
      <span class="freq-val">${(p.frequency * 100).toFixed(1)}%</span>
      <span class="freq-occ">${p.occurrences}×</span>
    </div>`).join('');
}

// ── Grafico comparativo de ambos patrones ─────────────────────────────────
function buildPatternsCompare(pd) {
    destroyChart('pat-compare');
    const up = pd.filter(p => p.pattern.includes('consecutive')).sort((a, b) => a.ticker.localeCompare(b.ticker));
    const sq = pd.filter(p => p.pattern.includes('squeeze')).sort((a, b) => a.ticker.localeCompare(b.ticker));
    const ctx = document.getElementById('ch-pat-compare').getContext('2d');
    S.charts['pat-compare'] = new Chart(ctx, {
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