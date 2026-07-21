"""
03_limpiar_datos.py
Tarea 3 del DAG: limpiar_datos

Para cada uno de los 5 archivos:
- Elimina duplicados
- Imputa valores nulos:
    * oil.csv -> interpolación lineal (precio del petróleo es una serie
      temporal continua; fines de semana/feriados no cotizan, así que
      interpolar entre el valor anterior y el siguiente es razonable).
    * Resto de campos -> moda (categóricos) o mediana (numéricos), según
      el criterio del estudiante, aplicado aquí de forma explícita y
      documentada.
- Corrige tipos de datos incorrectos (fechas como string -> Date,
  identificadores como float -> Int, etc.)
- Estandariza formato de fecha a Date (ISO) en todos los archivos.

Salida: parquet limpios en CLEANED_DIR, listos para la tarea 4 (consolidar).
También registra en el log cuántas filas se eliminaron por archivo, dato
que alimenta las "métricas del pipeline" exigidas en el README (sección 10).
"""

import sys
import json

import polars as pl

from config import PROCESSED_DIR, CLEANED_DIR, REPORTS_DIR, get_logger

logger = get_logger("limpiar_datos")

METRICAS_LIMPIEZA = REPORTS_DIR / "metricas_limpieza.json"


def _to_date(df: pl.DataFrame, col: str = "date") -> pl.DataFrame:
    if col not in df.columns:
        return df
    if df[col].dtype != pl.Date:
        df = df.with_columns(pl.col(col).str.to_date(strict=False).alias(col))
    return df


def limpiar_train(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    filas_antes = df.height
    # 'id' es una clave técnica autoincremental de Kaggle: nunca se repite,
    # así que se excluye de la comparación de duplicados de negocio (mismo
    # store_nbr + date + family + sales + onpromotion). Se conserva la
    # primera ocurrencia, descartando 'id' del criterio pero no de las
    # columnas finales.
    columnas_negocio = [c for c in df.columns if c != "id"]
    df = df.unique(subset=columnas_negocio, keep="first")
    duplicados_removidos = filas_antes - df.height

    df = _to_date(df)
    df = df.with_columns([
        pl.col("store_nbr").cast(pl.Int32, strict=False),
        pl.col("sales").cast(pl.Float64, strict=False),
        pl.col("onpromotion").cast(pl.Int32, strict=False),
        pl.col("family").cast(pl.Utf8),
    ])

    # sales no debería ser nulo; si lo es, se imputa con 0 (sin venta) en
    # lugar de mediana, porque un nulo en ventas diarias normalmente
    # representa "no hubo registro", equivalente a venta cero.
    nulos_sales = df["sales"].null_count()
    df = df.with_columns(pl.col("sales").fill_null(0.0))

    nulos_promo = df["onpromotion"].null_count()
    df = df.with_columns(pl.col("onpromotion").fill_null(0))

    metricas = {
        "duplicados_removidos": duplicados_removidos,
        "nulos_sales_imputados_con_0": nulos_sales,
        "nulos_onpromotion_imputados_con_0": nulos_promo,
        "filas_finales": df.height,
    }
    return df, metricas


def limpiar_stores(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    filas_antes = df.height
    df = df.unique()
    duplicados_removidos = filas_antes - df.height

    df = df.with_columns([
        pl.col("store_nbr").cast(pl.Int32, strict=False),
        pl.col("cluster").cast(pl.Int32, strict=False),
        pl.col("city").cast(pl.Utf8),
        pl.col("state").cast(pl.Utf8),
        pl.col("type").cast(pl.Utf8),
    ])

    # Categóricos: imputar con la moda si hubiera nulos (no se esperan,
    # pero se deja el criterio aplicado de forma defensiva).
    nulos_imputados = 0
    for col in ("city", "state", "type"):
        n = df[col].null_count()
        if n > 0:
            moda = df[col].mode()[0]
            df = df.with_columns(pl.col(col).fill_null(moda))
            nulos_imputados += n

    metricas = {
        "duplicados_removidos": duplicados_removidos,
        "nulos_categoricos_imputados_con_moda": nulos_imputados,
        "filas_finales": df.height,
    }
    return df, metricas


def limpiar_transactions(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    filas_antes = df.height
    df = df.unique()
    duplicados_removidos = filas_antes - df.height

    df = _to_date(df)
    df = df.with_columns([
        pl.col("store_nbr").cast(pl.Int32, strict=False),
        pl.col("transactions").cast(pl.Int64, strict=False),
    ])

    # Mediana para transacciones nulas (numérico, sensible a outliers en
    # fechas de alta demanda como Navidad, por eso mediana y no media).
    nulos = df["transactions"].null_count()
    if nulos > 0:
        mediana = df["transactions"].median()
        df = df.with_columns(pl.col("transactions").fill_null(mediana))

    metricas = {
        "duplicados_removidos": duplicados_removidos,
        "nulos_transactions_imputados_con_mediana": nulos,
        "filas_finales": df.height,
    }
    return df, metricas


def limpiar_oil(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    filas_antes = df.height
    df = df.unique()
    duplicados_removidos = filas_antes - df.height

    df = _to_date(df)
    df = df.sort("date")
    df = df.with_columns(pl.col("dcoilwtico").cast(pl.Float64, strict=False))

    nulos_antes = df["dcoilwtico"].null_count()
    # Interpolación lineal explícita, según lo pide la sección 5.1: el
    # precio del petróleo es una serie temporal -> interpolar entre el
    # valor previo y el siguiente válido es más correcto que usar
    # mediana/moda, que ignorarían la tendencia temporal.
    df = df.with_columns(pl.col("dcoilwtico").interpolate())
    # Si quedan nulos en los extremos de la serie (no hay valor previo o
    # siguiente para interpolar), se rellenan hacia adelante/atrás.
    df = df.with_columns(
        pl.col("dcoilwtico").fill_null(strategy="forward").fill_null(strategy="backward")
    )

    metricas = {
        "duplicados_removidos": duplicados_removidos,
        "nulos_oil_imputados_con_interpolacion_lineal": nulos_antes,
        "filas_finales": df.height,
    }
    return df, metricas


def limpiar_holidays(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    filas_antes = df.height
    df = df.unique()
    duplicados_removidos = filas_antes - df.height

    df = _to_date(df)
    df = df.with_columns([
        pl.col("type").cast(pl.Utf8),
        pl.col("locale").cast(pl.Utf8),
        pl.col("locale_name").cast(pl.Utf8),
        pl.col("description").cast(pl.Utf8),
        pl.col("transferred").cast(pl.Boolean, strict=False),
    ])

    nulos_imputados = 0
    for col in ("type", "locale", "locale_name", "description"):
        n = df[col].null_count()
        if n > 0:
            moda = df[col].mode()[0]
            df = df.with_columns(pl.col(col).fill_null(moda))
            nulos_imputados += n

    nulos_transferred = df["transferred"].null_count()
    df = df.with_columns(pl.col("transferred").fill_null(False))

    metricas = {
        "duplicados_removidos": duplicados_removidos,
        "nulos_categoricos_imputados_con_moda": nulos_imputados,
        "nulos_transferred_imputados_con_false": nulos_transferred,
        "filas_finales": df.height,
    }
    return df, metricas


LIMPIADORES = {
    "train": limpiar_train,
    "stores": limpiar_stores,
    "transactions": limpiar_transactions,
    "oil": limpiar_oil,
    "holidays": limpiar_holidays,
}


def main():
    CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    metricas_totales = {}

    for nombre, funcion_limpieza in LIMPIADORES.items():
        ruta_entrada = PROCESSED_DIR / f"raw_{nombre}.parquet"
        logger.info(f"Limpiando {nombre} ...")
        df = pl.read_parquet(ruta_entrada)

        df_limpio, metricas = funcion_limpieza(df)
        metricas_totales[nombre] = metricas

        ruta_salida = CLEANED_DIR / f"{nombre}_limpio.parquet"
        df_limpio.write_parquet(ruta_salida)
        logger.info(f"{nombre}: limpieza OK -> {ruta_salida} | {metricas}")

    with open(METRICAS_LIMPIEZA, "w", encoding="utf-8") as f:
        json.dump(metricas_totales, f, indent=2, ensure_ascii=False, default=str)

    logger.info(f"Métricas de limpieza guardadas en {METRICAS_LIMPIEZA}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
