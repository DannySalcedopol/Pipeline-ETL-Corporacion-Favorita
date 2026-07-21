"""
02_eda_inicial.py
Tarea 2 del DAG: eda_inicial

Genera el diagnóstico de calidad de cada uno de los 5 archivos ANTES de
limpiar:
- Número de filas y columnas
- Tipos de datos por columna
- Conteo (y %) de valores nulos por columna
- Conteo de filas duplicadas
- Rango de fechas (mín/máx) donde aplique

El resultado se guarda en un único reporte JSON (EDA_INICIAL_REPORT) para
que quede como evidencia y para alimentar la documentación del README.
"""

import sys
import json
from datetime import datetime, timezone

import polars as pl

from config import PROCESSED_DIR, EDA_INICIAL_REPORT, get_logger

logger = get_logger("eda_inicial")

ARCHIVOS = ["train", "stores", "transactions", "oil", "holidays"]


def perfil_columna(df: pl.DataFrame, columna: str) -> dict:
    serie = df[columna]
    nulos = serie.null_count()
    return {
        "tipo": str(serie.dtype),
        "nulos": nulos,
        "porcentaje_nulos": round(100 * nulos / df.height, 2) if df.height else 0.0,
    }


def rango_fechas(df: pl.DataFrame) -> dict | None:
    if "date" not in df.columns:
        return None
    try:
        col = df["date"]
        if col.dtype == pl.Utf8:
            col = col.str.to_date(strict=False)
        return {
            "minimo": str(col.min()),
            "maximo": str(col.max()),
        }
    except Exception as exc:
        logger.warning(f"No se pudo calcular rango de fechas: {exc}")
        return None


# Columnas a ignorar al buscar duplicados de negocio. 'id' en train.csv es
# una clave técnica autoincremental de Kaggle que nunca se repite, así que
# incluirla en la comparación impediría detectar filas que sí son
# duplicados reales (mismo store_nbr + date + family + sales).
COLUMNAS_IGNORAR_DUPLICADOS = {
    "train": ["id"],
}


def perfilar_archivo(nombre: str) -> dict:
    ruta = PROCESSED_DIR / f"raw_{nombre}.parquet"
    logger.info(f"Perfilando {nombre} desde {ruta} ...")
    df = pl.read_parquet(ruta)

    columnas_excluir = COLUMNAS_IGNORAR_DUPLICADOS.get(nombre, [])
    columnas_para_dup = [c for c in df.columns if c not in columnas_excluir]
    duplicados = df.height - df.unique(subset=columnas_para_dup).height

    perfil = {
        "archivo": nombre,
        "filas": df.height,
        "columnas": df.width,
        "columnas_detalle": {c: perfil_columna(df, c) for c in df.columns},
        "filas_duplicadas": duplicados,
        "porcentaje_duplicadas": round(100 * duplicados / df.height, 4) if df.height else 0.0,
        "rango_fechas": rango_fechas(df),
    }

    logger.info(
        f"{nombre}: {perfil['filas']} filas, {perfil['columnas']} columnas, "
        f"{duplicados} duplicados."
    )
    return perfil


def main():
    reporte = {
        "generado_en": datetime.now(timezone.utc).isoformat(),
        "etapa": "eda_inicial (antes de limpiar)",
        "archivos": {},
    }

    for nombre in ARCHIVOS:
        try:
            reporte["archivos"][nombre] = perfilar_archivo(nombre)
        except Exception as exc:
            logger.error(f"Fallo al perfilar '{nombre}': {exc}")
            raise

    EDA_INICIAL_REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(EDA_INICIAL_REPORT, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False, default=str)

    logger.info(f"Reporte EDA inicial guardado en {EDA_INICIAL_REPORT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
