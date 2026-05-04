// ============================================================
// utils.js — Utilidades compartidas
// ============================================================

import { API } from './config.js';
import { S }   from './state.js';

/**
 * Muestra un toast de notificacion en la esquina inferior derecha.
 * @param {string} msg   Texto del mensaje
 * @param {string} type  '' | 'success' | 'error'
 */
export function toast(msg, type = '') {
    const el = document.getElementById('toast');
    el.textContent = msg;
    el.className   = `show ${type}`;
    setTimeout(() => { el.className = ''; }, 3200);
}

/**
 * Fetch a un endpoint de la API. Lanza Error si la respuesta no es 2xx.
 * @param {string} path  Ruta relativa, ej: '/dashboard/tickers'
 * @returns {Promise<any>}
 */
export async function apiFetch(path) {
    const r = await fetch(API + path);
    if (!r.ok) throw new Error(`${r.status} ${path}`);
    return r.json();
}

/**
 * Destruye un Chart.js registrado en S.charts y lo elimina del estado.
 * @param {string} id  Clave usada en S.charts
 */
export function destroyChart(id) {
    if (S.charts[id]) {
        S.charts[id].destroy();
        delete S.charts[id];
    }
}