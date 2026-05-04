// ============================================================
// risk.js — Panel de Riesgo
// ============================================================

import { RISK_COLORS } from './config.js';
import { S }           from './state.js';
import { destroyChart } from './utils.js';

/**
 * Punto de entrada del panel. Usa los datos ya cargados en state.
 */
export function renderRiskPanel() {
    const rd = S.riskData;
    if (!rd || !rd.length) {
        document.getElementById('risk-table-wrap').innerHTML =
            '<div class="loader" style="color:var(--red)">risk_ranking.csv no encontrado.<br>Ejecuta el Req. 3 primero.</div>';
        return;
    }
    renderRiskTable(rd);
    buildRiskDonut(rd);
    buildRiskBar(rd);
}

// ── Tabla HTML con barra de volatilidad ──────────────────────────────────
function renderRiskTable(rd) {
    const maxV = Math.max(...rd.map(a => a.volatility));
    document.getElementById('risk-table-wrap').innerHTML = `
    <table class="risk-table">
      <thead><tr><th>#</th><th>Ticker</th><th>Volatilidad</th><th>Categoría</th></tr></thead>
      <tbody>${rd.map(a => `
        <tr>
          <td style="color:var(--text3);font-family:var(--mono);font-size:11px">${a.position}</td>
          <td style="font-family:var(--mono);font-weight:700">${a.ticker}</td>
          <td>
            <div class="vol-bar">
              <span style="font-family:var(--mono);font-size:11px;width:48px">${a.volatility.toFixed(1)}%</span>
              <div class="vol-bar-track">
                <div class="vol-bar-fill" style="width:${(a.volatility / maxV * 100).toFixed(1)}%;background:${RISK_COLORS[a.category]}"></div>
              </div>
            </div>
          </td>
          <td><span class="badge ${a.category}">${a.category}</span></td>
        </tr>`).join('')}
      </tbody>
    </table>`;
}

// ── Dona por categoria ────────────────────────────────────────────────────
function buildRiskDonut(rd) {
    destroyChart('risk-donut');
    const cnt = { conservador: 0, moderado: 0, agresivo: 0 };
    rd.forEach(a => cnt[a.category]++);
    const ctx = document.getElementById('ch-risk-donut').getContext('2d');
    S.charts['risk-donut'] = new Chart(ctx, {
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
                legend:  { position: 'bottom', labels: { padding: 16 } },
                tooltip: { callbacks: { label: c => `${c.label}: ${c.raw} activos` } },
            },
            cutout: '65%',
        },
    });
}

// ── Barras de volatilidad por activo ──────────────────────────────────────
function buildRiskBar(rd) {
    destroyChart('risk-bar');
    const ctx = document.getElementById('ch-risk-bar').getContext('2d');
    S.charts['risk-bar'] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: rd.map(a => a.ticker),
            datasets: [{
                label:           'Volatilidad Anualizada (%)',
                data:            rd.map(a => a.volatility),
                backgroundColor: rd.map(a => RISK_COLORS[a.category] + '88'),
                borderColor:     rd.map(a => RISK_COLORS[a.category]),
                borderWidth: 1, borderRadius: 4,
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { display: false }, ticks: { font: { family: 'Space Mono', size: 9 } } },
                y: { grid: { color: '#1a2d44' }, ticks: { callback: v => v + '%' } },
            },
        },
    });
}