from datetime import datetime
from pathlib import Path


# ============================================================
# CONFIGURACIÓN GENERAL DEL PROYECTO
# ============================================================

# Raíz del proyecto (compatible Mac / Linux)
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Carpetas principales
DATA_PATH = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_PATH / "raw"
PROCESSED_DATA_PATH = DATA_PATH / "processed"

# Crear carpetas si no existen (seguridad)
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================
# CONFIGURACIÓN DE ACTIVOS
# ============================================================

# Lista oficial de activos (mínimo 20 para entrega final)
ASSETS = [
    # Acciones USA
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",

    # ETFs
    "VOO",
    "SPY",
    "QQQ",
    "IWM",
    "EEM",

    # Acciones adicionales
    "TSLA",
    "NVDA",
    "JPM",
    "V",
    "UNH",

    # Índices o adicionales
    "^GSPC",
    "^IXIC",
    "XLF",
    "XLK",
    "XLE"
]


# ============================================================
# CONFIGURACIÓN DE FECHAS
# ============================================================

# Horizonte mínimo 5 años
END_DATE = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today().replace(year=datetime.today().year - 5)).strftime("%Y-%m-%d")


# ============================================================
# CONFIGURACIÓN DE YAHOO FINANCE
# ============================================================

YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"


# ============================================================
# CONFIGURACIÓN DE LIMPIEZA
# ============================================================

# Política para valores faltantes
DROP_ROWS_WITH_NULLS = True

# Columnas obligatorias
REQUIRED_COLUMNS = [
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume"
]

# Eliminar columna Adj Close si viene vacía
REMOVE_ADJ_CLOSE = True


# ============================================================
# CONFIGURACIÓN DEL PIPELINE
# ============================================================

# Pausa entre requests (evitar bloqueos)
REQUEST_SLEEP_SECONDS = 2

# Nombre de archivos finales
UNIFIED_DATASET_FILENAME = "unified_dataset.csv"
RETURNS_DATASET_FILENAME = "returns_dataset.csv"