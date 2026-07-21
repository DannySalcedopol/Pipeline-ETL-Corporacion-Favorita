"""
01_cargar_datos.py
Tarea 1 del DAG: cargar_datos

Lee los cinco archivos CSV del dataset Store Sales - Corporación Favorita
desde la carpeta local de datos de la VM, usando Polars (scan_csv / read_csv).

Si alguna lectura falla (archivo ausente, CSV corrupto, esquema inesperado),
la tarea lanza una excepción. Airflow marca la tarea como fallida y, gracias
a la dependencia secuencial del DAG, las tareas siguientes no se ejecutan.

Salida: cada archivo se vuelca a Parquet en PROCESSED_DIR como una validación
rápida + control intermedio de lectura (no es la limpieza, solo asegura que
los 5 CSV fueron leídos correctamente).
"""

import sys
import polars as pl

from config import RAW_FILES, PROCESSED_DIR, get_logger

logger = get_logger("cargar_datos")

# Esquemas esperados (tipos base) para detectar problemas de tipos lo antes
# posible. Polars infiere tipos por defecto, pero forzar columnas clave evita
# sorpresas (p.ej. store_nbr leído como string en algún CSV mal formado).
EXPECTED_COLUMNS = {
    "train": {"id", "date", "store_nbr", "family", "sales", "onpromotion"},
    "stores": {"store_nbr", "city", "state", "type", "cluster"},
    "transactions": {"date", "store_nbr", "transactions"},
    "oil": {"date", "dcoilwtico"},
    "holidays": {"date", "type", "locale", "locale_name", "description", "transferred"},
}


def cargar_archivo(nombre: str, ruta) -> pl.DataFrame:
    """Lee un CSV con Polars y valida que tenga las columnas esperadas."""
    if not ruta.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo '{nombre}' en la ruta esperada: {ruta}. "
            f"Verifica que el dataset esté descargado en la carpeta local de la VM."
        )

    logger.info(f"Leyendo {nombre} desde {ruta} ...")
    try:
        df = pl.read_csv(ruta, try_parse_dates=True, infer_schema_length=10000)
    except Exception as exc:
        raise RuntimeError(f"Fallo al leer '{nombre}': {exc}") from exc

    columnas_actuales = set(df.columns)
    columnas_esperadas = EXPECTED_COLUMNS[nombre]
    faltantes = columnas_esperadas - columnas_actuales
    if faltantes:
        raise ValueError(
            f"El archivo '{nombre}' no tiene las columnas esperadas. "
            f"Faltan: {faltantes}. Columnas encontradas: {columnas_actuales}"
        )

    logger.info(f"{nombre}: {df.height} filas, {df.width} columnas. OK.")
    return df


def main():
    dataframes = {}
    errores = []

    for nombre, ruta in RAW_FILES.items():
        try:
            dataframes[nombre] = cargar_archivo(nombre, ruta)
        except Exception as exc:
            logger.error(str(exc))
            errores.append(nombre)

    if errores:
        # Cualquier fallo de lectura detiene la tarea -> Airflow no continúa
        # con eda_inicial ni con el resto del DAG.
        raise RuntimeError(
            f"La carga de datos falló para los archivos: {errores}. "
            f"Se detiene el pipeline."
        )

    # Persistimos cada DataFrame en Parquet como checkpoint de la tarea 1.
    # Parquet es más rápido y compacto que CSV para las tareas siguientes.
    for nombre, df in dataframes.items():
        salida = PROCESSED_DIR / f"raw_{nombre}.parquet"
        df.write_parquet(salida)
        logger.info(f"Checkpoint escrito: {salida}")

    logger.info("Tarea cargar_datos completada exitosamente para los 5 archivos.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
