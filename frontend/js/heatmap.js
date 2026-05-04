// ============================================================
// heatmap.js — Panel de Correlacion
// ============================================================

import { apiFetch } from './utils.js';

let _loaded = false;

/**
 * Carga la matriz de correlacion desde la API (lazy: solo la primera vez).
 */
export async function loadHeatmap() {
    if (_loaded) return;
    const el = document.getElementById('heatmap-container');
    el.innerHTML = '<div class="loader"><div class="spinner"></div>Calculando correlaciones Pearson...</div>';
    try {
        const data = await apiFetch('/dashboard/correlation');
        _loaded = true;
        renderHeatmap(data);
    } catch (e) {
        el.innerHTML = `<div class="loader" style="color:var(--red)">Error: ${e.message}</div>`;
    }
}

// ── Interpolacion de color r ∈ [-1,1] → rojo/gris/verde ──────────────────
function lerp(t, a, b) { return a + t * (b - a); }

function corrColor(v) {
    const t = (v + 1) / 2;
    if (t < 0.5) {
        const u = t * 2;
        return `rgb(${Math.round(lerp(u, 231, 149))},${Math.round(lerp(u, 76, 165))},${Math.round(lerp(u, 60, 166))})`;
    } else {
        const u = (t - 0.5) * 2;
        return `rgb(${Math.round(lerp(u, 149, 46))},${Math.round(lerp(u, 165, 230))},${Math.round(lerp(u, 166, 118))})`;
    }
}

// ── Renderizado del heatmap como grid HTML ────────────────────────────────
function renderHeatmap({ tickers, matrix }) {
    const n = tickers.length, cell = 44;
    let html = `<div class="heatmap-grid" style="grid-template-columns:${cell}px repeat(${n},${cell}px)">`;

    // Cabecera (columnas)
    html += `<div style="width:${cell}px;height:${cell}px"></div>`;
    tickers.forEach(t => {
        html += `<div class="heatmap-label" style="height:${cell}px;font-size:8px;writing-mode:vertical-lr;transform:rotate(180deg);padding:3px 0">${t}</div>`;
    });

    // Filas de datos
    matrix.forEach((row, i) => {
        html += `<div class="heatmap-label" style="height:${cell}px;justify-content:flex-end;padding-right:5px;font-size:9px">${tickers[i]}</div>`;
        row.forEach((val, j) => {
            const bg = corrColor(val);
            const fg = Math.abs(val) > 0.45 ? '#000' : '#cdd9e5';
            html += `<div class="heatmap-cell" style="background:${bg};color:${fg}" title="${tickers[i]}×${tickers[j]}: ${val.toFixed(2)}">${i === j ? '1.00' : val.toFixed(2)}</div>`;
        });
    });

    html += '</div>';
    document.getElementById('heatmap-container').innerHTML = html;
}