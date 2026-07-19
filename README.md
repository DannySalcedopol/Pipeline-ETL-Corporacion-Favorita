# Pipeline ETL Corporación Favorita

## 1. Descripción del proyecto

Pipeline ETL desarrollado para el procesamiento y análisis de datos de ventas de Corporación Favorita.

El proyecto implementa un flujo automatizado utilizando Apache Airflow como herramienta de orquestación y Polars como motor principal para la transformación eficiente de grandes volúmenes de datos.

El pipeline permite realizar la extracción de datos desde archivos CSV, limpieza y transformación de información, consolidación en formato Parquet, generación de análisis exploratorio y exportación hacia PostgreSQL para su consumo mediante herramientas de Business Intelligence como Power BI.

### Tecnologías utilizadas

| Tecnología            | Uso                                           |
| --------------------- | --------------------------------------------- |
| Python                | Desarrollo del pipeline ETL                   |
| Apache Airflow 2.10.5 | Orquestación y ejecución automática del flujo |
| Polars                | Limpieza y transformación de datos            |
| Pandas / NumPy        | Análisis complementario                       |
| PyArrow               | Manejo de archivos Parquet                    |
| PostgreSQL            | Almacenamiento analítico                      |
| Power BI              | Visualización de resultados                   |

---

# 2. Archivos del dataset y rol en el pipeline

Los datos utilizados corresponden al conjunto de ventas de Corporación Favorita.

| Archivo             | Descripción                                        | Uso en el pipeline                                       |
| ------------------- | -------------------------------------------------- | -------------------------------------------------------- |
| train.csv           | Registro histórico de ventas por producto y tienda | Fuente principal de ventas                               |
| test.csv            | Datos destinados a predicción                      | Dataset complementario                                   |
| stores.csv          | Información de tiendas                             | Enriquecimiento de ventas con ubicación y tipo de tienda |
| transactions.csv    | Número de transacciones realizadas por tienda      | Análisis del comportamiento comercial                    |
| oil.csv             | Precio diario del petróleo                         | Análisis de relación entre economía y ventas             |
| holidays_events.csv | Información de feriados y eventos                  | Evaluación del impacto de fechas especiales              |

Los archivos originales se almacenan localmente y no se incluyen en el repositorio debido a su tamaño.

---

# 3. Arquitectura de la solución

```text
Dataset CSV
     |
     v
Apache Airflow
     |
     v
DAG favorita_pipeline
     |
     +----------------+
     |                |
     v                v
Carga datos     Limpieza Polars
     |                |
     +----------------+
             |
             v
     Consolidación Parquet
             |
             v
    Análisis Exploratorio
             |
             v
        PostgreSQL
             |
             v
          Power BI
```

---

# 4. Descripción del DAG

El DAG principal se encuentra en:

```text
dags/favorita_pipeline.py
```

Nombre del DAG:

```text
favorita_pipeline
```

El flujo está compuesto por las siguientes tareas:

| Tarea             | Descripción                                               |
| ----------------- | --------------------------------------------------------- |
| cargar_datos      | Lectura de archivos CSV y preparación inicial             |
| eda_inicial       | Generación de métricas iniciales y revisión de datos      |
| limpiar_datos     | Tratamiento de valores faltantes, tipos e inconsistencias |
| consolidar        | Unión de datasets y generación de archivos Parquet        |
| eda_profundo      | Creación de tablas analíticas                             |
| exportar_postgres | Carga del resultado final hacia PostgreSQL                |

### Dependencias

```text
cargar_datos
      |
      v
eda_inicial
      |
      v
limpiar_datos
      |
      v
consolidar
      |
      v
eda_profundo
      |
      v
exportar_postgres
```

### Configuración

El DAG utiliza:

* PythonOperator para ejecutar scripts.
* Variables de configuración mediante archivos Python.
* PostgreSQL como almacenamiento final.
* Archivos Parquet como formato intermedio optimizado.

---

# 5. Proceso del pipeline

## Etapa 1: Carga de datos

Se cargan los archivos CSV:

* train.csv
* test.csv
* stores.csv
* transactions.csv
* oil.csv
* holidays_events.csv

Los datos son convertidos a estructuras eficientes para procesamiento.

---

## Etapa 2: Análisis exploratorio inicial

Se generan métricas iniciales:

* cantidad de registros;
* tipos de datos;
* valores nulos;
* estadísticas generales.

Captura del DAG ejecutándose en Apache Airflow:

![Vista Graph Airflow](capturas/01_graph.png)
---

## Etapa 3: Limpieza de datos

Proceso realizado mediante Polars:

* tratamiento de valores faltantes;
* conversión de tipos;
* eliminación de inconsistencias;
* preparación de datasets limpios.

Captura:

```
[Agregar captura tarea limpieza en Airflow]
```

---

## Etapa 4: Consolidación

Se integran las fuentes:

* ventas;
* tiendas;
* transacciones;
* petróleo;
* feriados.

El resultado se almacena en formato Parquet.

Captura:

```
[Agregar captura consolidación]
```

---

## Etapa 5: Análisis exploratorio profundo

Se generan tablas analíticas:

* evolución de ventas;
* ventas por tienda;
* ventas por familia;
* efecto de promociones;
* impacto de feriados;
* relación con precio del petróleo.

---

## Etapa 6: Exportación

Los datos consolidados son enviados hacia PostgreSQL para su análisis mediante Power BI.

Capturas del almacenamiento y exportación en PostgreSQL:

![Tablas PostgreSQL](capturas/03_tablas_postgres.png)

![Exportación PostgreSQL](capturas/04_exportacion.png)


---

# 6. Métricas del pipeline

## Registros procesados

| Dataset             | Registros |
| ------------------- | --------: |
| train.csv           | 3,000,888 |
| stores.csv          |        54 |
| transactions.csv    |    83,488 |
| oil.csv             |     1,218 |
| holidays_events.csv |       350 |

## Métricas de limpieza

| Proceso              | Resultado                 |
| -------------------- | ------------------------- |
| Valores faltantes    | Tratados durante limpieza |
| Tipos incorrectos    | Convertidos               |
| Datos inconsistentes | Corregidos                |
| Duplicados           | Eliminados                |

## Tiempo de ejecución

| Tarea             | Tiempo                    |
| ----------------- | ------------------------- |
| cargar_datos      | Pendiente captura Airflow |
| eda_inicial       | Pendiente captura Airflow |
| limpiar_datos     | Pendiente captura Airflow |
| consolidar        | Pendiente captura Airflow |
| eda_profundo      | Pendiente captura Airflow |
| exportar_postgres | Pendiente captura Airflow |


Vista Gantt del tiempo de ejecución de las tareas:

![Gantt Airflow](capturas/02_gantt.png)

---

# 7. Dashboard Power BI

El resultado del pipeline es utilizado para construir dashboards analíticos.

Indicadores principales:

* evolución de ventas;
* ventas por tienda;
* ventas por familia;
* impacto de promociones;
* comportamiento según feriados;
* análisis económico relacionado con petróleo.

Capturas:

```
[Agregar captura Dashboard Power BI]
```

---

# 8. Despliegue del proyecto

## Requisitos

* Python 3.12
* Apache Airflow 2.10.5
* PostgreSQL
* Entorno virtual Python

## Instalación

Crear entorno virtual:

```bash
python3 -m venv venv
```

Activar:

```bash
source venv/bin/activate
```

Instalar dependencias:

```bash
pip install -r requirements.txt
```

Configurar variables de entorno:

```text
.env
```

Ejecutar Airflow:

Terminal 1:

```bash
airflow webserver
```

Terminal 2:

```bash
airflow scheduler
```

Ejecutar DAG:

```bash
airflow dags trigger favorita_pipeline
```

---

# 9. Conclusiones y recomendaciones

El desarrollo del pipeline permitió automatizar el procesamiento de información de ventas mediante una arquitectura ETL completa.

El uso de Apache Airflow facilitó la programación y monitoreo de tareas, mientras que Polars permitió manejar grandes volúmenes de información con eficiencia.

Como recomendaciones futuras:

* implementar control de calidad automático de datos;
* agregar alertas de fallos en Airflow;
* incorporar almacenamiento cloud;
* automatizar actualizaciones del dashboard;
* implementar modelos predictivos sobre ventas.

---

# Estructura del proyecto

```text
ProyectoAnalisis/

├── dags/
│   └── favorita_pipeline.py

├── scripts/
│   ├── 01_cargar_datos.py
│   ├── 02_eda_inicial.py
│   ├── 03_limpiar_datos.py
│   ├── 04_consolidar.py
│   ├── 05_eda_profundo.py
│   ├── 06_exportar_postgres.py
│   └── config.py

├── requirements.txt
├── manifest.json
└── README.md
```

---

# Autores

* Damian Cadavid
* Danny Salcedo
* Dereck Ortiz

Proyecto académico - Pipeline ETL Corporación Favorita
