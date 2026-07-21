"""
config.py
Configuración compartida por todos los scripts del pipeline favorita_pipeline.

Centraliza:
- Rutas de archivos (carpeta local de datos en la VM, fuera del repo)
- Credenciales de PostgreSQL (vía variables de entorno, nunca hardcodeadas)
- Logging consistente para que Airflow capture mensajes uniformes

IMPORTANTE: los archivos CSV del dataset NUNCA se suben al repositorio.
Viven únicamente en DATA_DIR, dentro del sistema de archivos de la VM
(local o de Azure).
"""

import os
import logging
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
# Detecta la raíz del proyecto (carpeta donde está scripts/)
_CONFIG_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _CONFIG_DIR.parent

# Carpeta local de datos. Por defecto, favorita_data/ en la raíz del repo.
DATA_DIR = Path(os.environ.get("FAVORITA_DATA_DIR", PROJECT_ROOT / "data"))

RAW_DIR = DATA_DIR
PROCESSED_DIR = PROJECT_ROOT / "favorita_data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "favorita_data" / "reports"

for d in (RAW_DIR, PROCESSED_DIR, REPORTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Nombres de archivo esperados del dataset (sección 3.2 del proyecto)
RAW_FILES = {
    "train": RAW_DIR / "train.csv",
    "stores": RAW_DIR / "stores.csv",
    "transactions": RAW_DIR / "transactions.csv",
    "oil": RAW_DIR / "oil.csv",
    "holidays": RAW_DIR / "holidays_events.csv",
}

# Archivo consolidado intermedio (salida de la tarea 4 - consolidar)
CONSOLIDATED_PARQUET = PROCESSED_DIR / "consolidado.parquet"
CLEANED_DIR = PROCESSED_DIR / "limpios"  # parquet limpios por archivo (tarea 3)

EDA_INICIAL_REPORT = REPORTS_DIR / "eda_inicial.json"
EDA_PROFUNDO_REPORT = REPORTS_DIR / "eda_profundo.json"

# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------
# Credenciales vía variables de entorno (definidas en el .env de la VM o en
# la configuración de Airflow Connections). Nunca hardcodear usuario/clav
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "favorita_db"
PG_USER = "favorita_user"
PG_PASSWORD = "Favorita2026!"

PG_CONN_STRING = (
    f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def get_logger(name: str) -> logging.Logger:
    """Logger uniforme para todos los scripts. Airflow captura este output
    automáticamente en los logs de cada tarea."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
