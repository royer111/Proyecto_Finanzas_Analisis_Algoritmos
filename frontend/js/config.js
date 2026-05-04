// ============================================================
// config.js — Constantes globales y configuracion de Chart.js
// ============================================================

export const API = 'http://localhost:8000/api';

export const RISK_COLORS = {
    conservador: '#00e676',
    moderado:    '#ffab40',
    agresivo:    '#ff4444',
};

/**
 * Aplica los defaults globales de Chart.js.
 * Debe llamarse desde main.js DESPUÉS de que Chart esté disponible en window.
 */
export function applyChartDefaults() {
    if (typeof Chart === 'undefined') return;
    Chart.defaults.color                            = '#768d9e';
    Chart.defaults.font.family                      = 'DM Sans';
    Chart.defaults.plugins.tooltip.backgroundColor = '#111d2e';
    Chart.defaults.plugins.tooltip.borderColor     = '#1a2d44';
    Chart.defaults.plugins.tooltip.borderWidth     = 1;
    Chart.defaults.plugins.tooltip.titleColor      = '#cdd9e5';
    Chart.defaults.plugins.tooltip.bodyColor       = '#768d9e';
}