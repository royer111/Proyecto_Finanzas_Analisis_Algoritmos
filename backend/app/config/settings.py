from pathlib import Path

# ==========================================================
# CONFIGURACIÓN GENERAL DEL PROYECTO
# ==========================================================

# Ruta raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Rutas de almacenamiento de datos
DATA_PATH = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_PATH / "raw"
PROCESSED_DATA_PATH = DATA_PATH / "processed"
MERGED_DATA_PATH = DATA_PATH / "merged"

# Crear carpetas si no existen
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)
MERGED_DATA_PATH.mkdir(parents=True, exist_ok=True)

# ==========================================================
# CONFIGURACIÓN DE DESCARGA (EXTRACT)
# ==========================================================

# Endpoint oficial estable de Yahoo Finance
YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

# Rango mínimo de 5 años
START_DATE = "2019-01-01"
END_DATE = "2024-01-01"

# Intervalo de espera entre requests (evita bloqueos)
REQUEST_SLEEP_SECONDS = 1

# ==========================================================
# PORTAFOLIO (MÍNIMO 20 ACTIVOS)
# ==========================================================

ASSETS = [

    # ETFs Globales
    "VOO",      # Vanguard S&P 500 ETF
    "CSPX.L",   # iShares Core S&P 500 (London)
    "QQQ",
    "VTI",
    "EFA",
    "IEMG",
    "GLD",
    "TLT",
    "XLF",
    "XLK",

    # Acciones USA
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "NVDA",
    "TSLA",

    # Acciones Colombianas (Yahoo usa .BO para BVC)
   # "ECOPETROL.BO",
    #"ISA.BO",
    #"GEB.BO"
]

# ==========================================================
# CONFIGURACIÓN DE LIMPIEZA (TRANSFORM)
# ==========================================================

# Columnas obligatorias
REQUIRED_COLUMNS = [
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume"
]

# Manejo de valores faltantes
DROP_ROWS_WITH_NULLS = True

# Umbral de detección de anomalías (retorno diario absoluto)
# 0.30 = 30%
ANOMALY_THRESHOLD = 0.30

# ==========================================================
# CONFIGURACIÓN DE UNIFICACIÓN (LOAD)
# ==========================================================

# Si True → solo fechas donde TODOS los activos tengan datos
STRICT_DATE_ALIGNMENT = True