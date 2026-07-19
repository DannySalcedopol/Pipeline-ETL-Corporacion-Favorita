"""
06_exportar_postgres.py
Tarea 6 del DAG: exportar_postgres

Escribe en PostgreSQL:
1. El DataFrame limpio y consolidado completo (tabla ventas_consolidado).
2. Todas las tablas de estadísticos generadas por eda_profundo
   (prefijo eda_) para que Power BI pueda consultarlas directamente.
3. El reporte de EDA inicial y las métricas de limpieza, aplanados a
   tablas simples, para trazabilidad de calidad de datos en el dashboard.

Usa Polars `write_database` (vía SQLAlchemy/psycopg2 por debajo). Cada
tabla se reemplaza por completo (if_table_exists="replace") porque el
pipeline se ejecuta de forma completa cada vez (no incremental), de modo
que Power BI siempre refleja la última corrida.
"""

import sys
import json
import time
from sqlalchemy import create_engine
import polars as pl

from config import (
    CONSOLIDATED_PARQUET,
    REPORTS_DIR,
    EDA_INICIAL_REPORT,
    PG_CONN_STRING,
    get_logger,
    PROCESSED_DIR,
)

logger = get_logger("exportar_postgres")

EDA_TABLES_DIR = REPORTS_DIR / "eda_tables"
METRICAS_LIMPIEZA = REPORTS_DIR / "metricas_limpieza.json"

DB_FALLBACK_DIR = PROCESSED_DIR / "db_export"


def _get_connection() -> tuple[str, str]:
    """Intenta conectar a PostgreSQL. Si falla, usa SQLite local como fallback.
    Retorna (connection_string, engine_type: "postgresql" | "sqlite")."""
    import urllib.parse

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=urllib.parse.urlparse(PG_CONN_STRING).hostname or "localhost",
            port=urllib.parse.urlparse(PG_CONN_STRING).port or 5432,
            dbname=urllib.parse.urlparse(PG_CONN_STRING).path.lstrip("/"),
            user=urllib.parse.urlparse(PG_CONN_STRING).username or "favorita_user",
            password=urllib.parse.urlparse(PG_CONN_STRING).password or "",
            connect_timeout=3,
        )
        conn.close()
        logger.info("Usando PostgreSQL como destino.")
        return PG_CONN_STRING, "postgresql"
    except Exception as exc:
        DB_FALLBACK_DIR.mkdir(parents=True, exist_ok=True)
        sqlite_path = DB_FALLBACK_DIR / "favorita.db"
        if sqlite_path.exists():
            sqlite_path.unlink()
        logger.warning(
            f"No se pudo conectar a PostgreSQL ({exc}). "
            f"Usando SQLite local como fallback: {sqlite_path}"
        )
        return f"sqlite:///{sqlite_path}?timeout=60", "sqlite"

from sqlalchemy import create_engine

CHUNK_SIZE = 100_000

import psycopg2
from io import StringIO

CHUNK_SIZE = 100_000

def escribir_tabla(df: pl.DataFrame, nombre_tabla: str, conn_str: str):

    import urllib.parse

    url = urllib.parse.urlparse(conn_str)

    conn = psycopg2.connect(
        host=url.hostname,
        port=url.port,
        dbname=url.path.lstrip("/"),
        user=url.username,
        password=url.password,
    )

    cur = conn.cursor()

    # Crear tabla desde el primer bloque
    primer = df.head(CHUNK_SIZE).to_pandas()

    from sqlalchemy import create_engine
    engine = create_engine(conn_str)

    primer.head(0).to_sql(
        nombre_tabla,
        engine,
        if_exists="replace",
        index=False,
    )

    total = df.height

    for i in range(0, total, CHUNK_SIZE):

        print(f"{nombre_tabla}: {i:,} / {total:,}")

        chunk = df.slice(i, CHUNK_SIZE).to_pandas()

        buffer = StringIO()
        chunk.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cur.copy_expert(
            f"COPY {nombre_tabla} FROM STDIN WITH CSV",
            buffer,
        )

        conn.commit()

    cur.close()
    conn.close()

def main():
    conn_str, engine_type = _get_connection()

    # 1) Consolidado principal
    logger.info("Exportando tabla principal: ventas_consolidado ...")
    consolidado = pl.read_parquet(CONSOLIDATED_PARQUET)
    escribir_tabla(consolidado, "ventas_consolidado", conn_str)

    # 2) Tablas de EDA profundo (todas las generadas en la tarea 5)
    if EDA_TABLES_DIR.exists():
        archivos_eda = sorted(EDA_TABLES_DIR.glob("*.parquet"))
        logger.info(f"Exportando {len(archivos_eda)} tablas de EDA profundo ...")
        for archivo in archivos_eda:
            nombre_tabla = f"eda_{archivo.stem}"
            df_eda = pl.read_parquet(archivo)
            escribir_tabla(df_eda, nombre_tabla, conn_str)
    else:
        logger.warning(
            f"No se encontró {EDA_TABLES_DIR}. ¿Se ejecutó eda_profundo antes?"
        )

    # 3) Calidad de datos: EDA inicial (aplanado a una fila por archivo)
    if EDA_INICIAL_REPORT.exists():
        with open(EDA_INICIAL_REPORT, "r", encoding="utf-8") as f:
            eda_inicial = json.load(f)

        filas = []
        for nombre_archivo, datos in eda_inicial.get("archivos", {}).items():
            filas.append(
                {
                    "archivo": nombre_archivo,
                    "filas": datos["filas"],
                    "columnas": datos["columnas"],
                    "filas_duplicadas": datos["filas_duplicadas"],
                    "porcentaje_duplicadas": datos["porcentaje_duplicadas"],
                    "fecha_minima": (datos.get("rango_fechas") or {}).get("minimo"),
                    "fecha_maxima": (datos.get("rango_fechas") or {}).get("maximo"),
                }
            )
        df_calidad = pl.DataFrame(filas)
        escribir_tabla(df_calidad, "calidad_datos_inicial", conn_str)
    else:
        logger.warning(f"No se encontró {EDA_INICIAL_REPORT}.")

    # 4) Métricas de limpieza (para la sección "Métricas del pipeline" del README)
    if METRICAS_LIMPIEZA.exists():
        with open(METRICAS_LIMPIEZA, "r", encoding="utf-8") as f:
            metricas = json.load(f)

        filas = []
        for archivo, valores in metricas.items():
            fila = {"archivo": archivo}
            fila.update(valores)
            filas.append(fila)
        df_metricas = pl.DataFrame(filas)
        escribir_tabla(df_metricas, "metricas_limpieza", conn_str)
    else:
        logger.warning(f"No se encontró {METRICAS_LIMPIEZA}.")

    logger.info("Tarea exportar_postgres completada. Power BI ya puede leer las tablas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
