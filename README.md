# 🧹 Data Cleaning Pipeline

[![Polars](https://img.shields.io/badge/Polars-Fast__DataFrames-red.svg)](https://pola.rs/)
[![Prefect](https://img.shields.io/badge/Prefect-Workflow__Orchestration-blue.svg)](https://www.prefect.io/)
[![Pydantic](https://img.shields.io/badge/Pydantic-Data__Validation-green.svg)](https://pydantic-docs.helpmanual.io/)
[![Python](https://img.shields.io/badge/Python-3.8+-yellow.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Un sistema completo y automatizado para limpieza de datos que integra análisis exploratorio, gestión inteligente de memoria y pipelines robustos de preprocesamiento con validación de datos.

## 📚 Sistema Profesional de Limpieza de Datos

Este repositorio contiene un pipeline de limpieza de datos diseñado para manejar datasets grandes con eficiencia, validación robusta y procesamiento optimizado.

## 🏗️ Estructura del Repositorio

### 🔍 **01 - Análisis Exploratorio**

| Módulo       | Tecnologías     | Funcionalidades                                                         |
|--------------|-----------------|-------------------------------------------------------------------------|
| **`EDA.py`** | Polars, Logging | Análisis automático, estadísticas, detección de nulos, vista preliminar |

### 💾 **02 - Gestión de Memoria Inteligente**  

| Módulo            | Tecnologías              | Funcionalidades                                         |
|-------------------|--------------------------|---------------------------------------------------------|
| **`GetFrame.py`** | Polars, Psutil, Pydantic | Decisión automática Eager/Lazy, optimización de memoria |

### 🚀 **03 - Pipeline de Limpieza**

| Módulo                     | Tecnologías               | Funcionalidades                                             |
|----------------------------|---------------------------|-------------------------------------------------------------|
| **`DataPreProcessing.py`** | Polars, Prefect, Pydantic | Renombrado, tipado, manejo de nulos, imputación inteligente |

### ⚙️ **04 - Configuración y Validación**

| Módulo                                 | Tecnologías          | Funcionalidades                                 |
|----------------------------------------|----------------------|-------------------------------------------------|
| **`ReadFile.py`** `ValidatorConfig.py` | Pydantic, YAML, TOML | Validación de configuraciones, schemas de datos |

### 🎯 **05 - Estrategias y Patrones**

| Módulo              | Tecnologías | Funcionalidades                                        |
|---------------------|-------------|--------------------------------------------------------|
| **`Strategies.py`** | Enum        | Estrategias de imputación, tipos de datos, operaciones |

## 🎯 Características Principales

### 🧠 **Gestión Inteligente de Memoria**

```python
def decision_frame(self) -> str: 
    tamaño_archivo = self.archivo.stat().st_size
    memoria_disponible = psutil.virtual_memory().available
    
    # Decisión automática Eager vs Lazy basada en recursos
    if tamaño_archivo < memoria_disponible * 0.1: 
        return 'eager'
    elif tamaño_archivo < memoria_disponible * 0.75: 
        return 'lazy'
```

### 🔧 **Manejo Avanzado de Valores Nulos**

```python 
def pipeline_null_handler(self, columnas_representativas: List[str]) -> pl.LazyFrame:
    # Análisis jerárquico de nulos por columnas representativas
    frame_nuevo = AnalysisNullData(
        frame=frame, 
        model=self.model, 
        columnas_representativas=columnas_representativas, 
        columnas_target=self.analyse_null
    )
    return frame_nuevo.analysis_null_data()
```

### 📊 **Análisis Exploratorio Automatizado**

```python 
def EDA_Basico(self) -> None: 
    self.primera_vista()        # Primeras 10 filas
    self.info_general()         # Metadatos del dataset
    self.estadistica_general()  # Estadísticas descriptivas
    self.valores_nulos()        # Análisis de valores missing
```

## 🚀 Ejemplos de Implementación

### 🔍 **Análisis Exploratorio Rápido**

```python 
# Análisis completo con una línea
eda = EDAGeneral(frame=df_lazy)
eda.EDA_Basico()

# Output automático:
# - Primeras 10 filas
# - Shape y tipos de datos
# - Estadísticas descriptivas
# - Análisis de valores nulos
```

### 🧹 **Pipeline Completo de Limpieza**

```python 
# Pipeline automatizado con Prefect
cleaning_pipeline = DataCleaning(frame=df, model=config_model)

df_limpio = cleaning_pipeline.pipeline_data_cleaning(
    columas_representativas=['categoria', 'grupo'],
    umbral=0.4,
    min_proportion=45
)
```

### ⚡ **Configuración con Validación**

```yaml
Paths: 
  input_file: "datos.csv"
  output_file: "datos_limpios.parquet"

Cleaning_Rules: 
  column_rename: {'Name':'name', 'Platform':'platform'}
  dtype_override: {'year_of_release': 'Int32', 'sales': 'Float32'}
  null_values: 
    null_val_csv: ['tbd', 'TBD', 'N/A', 'nan']
    null_imput_num_operation: 'median'
    null_imput_cat_operation: 'mode'
```

## 📦 Instalación y Uso

### **Requisitos**

```bash 
# requirements.txt
polars>=0.19.0
prefect>=2.0.0
pydantic>=2.0.0
pyyaml>=6.0
tomli>=2.0.0
psutil>=5.9.0
```

### **Uso Rápido**

```bash 
# Clonar el repositorio
git clone https://github.com/sm7ss/data-cleaning-pipeline.git
cd data-cleaning-pipeline

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar limpieza completa
python -c "
from ReadFile import ReadConfig
from GetFrame import FrameCollector
from DataPreProcessing import DataCleaning

config = ReadConfig('config.yaml').read_config()
frame = FrameCollector(model=config).get_frame()
cleaning = DataCleaning(frame=frame, model=config)
resultado = cleaning.pipeline_data_cleaning(['categoria'])
print('Limpieza completada!')
"
```

## 🛠 Arquitectura y Tecnologías

| Tecnología     | Versión | Propósito                         |
|----------------|---------|-----------------------------------|
| **Polars**	 | 0.19+   | Procesamiento rápido de datos     |
| **Prefect**	 | 2.0+	   | Orchestration de pipelines        |
| **Pydantic**	 | 2.0+	   | Validación de configuraciones     |
| **Psutil**	 | 5.9+	   | Monitoreo de recursos del sistema |
| **Python**	 | 3.8+	   | Lenguaje base con type hints      |

## 🔍 Características Avanzadas

### **1. Análisis Inteligente de Nulos**

- Detección automática de patrones de missing values
- Imputación estratégica basada en columnas representativas
- Umbrales configurables para eliminación vs imputación
- Análisis jerárquico por grupos de datos

### **2. Optimización de Rendimiento**

- Decisión automática Eager vs Lazy
- Gestión de memoria basada en recursos del sistema
- Procesamiento por lotes para datasets grandes
- Streaming processing para optimización

### **3. Validación Robusta**

- Schemas de datos con Pydantic
- Validación de configuraciones YAML/TOML
- Verificación de columnas y tipos de datos
- Manejo de errores profesional con logging

## 📈 Casos de Uso Implementados

### 🧹 **Limpieza de Datos de Juegos**

- Normalización de nombres de columnas
- Tipado correcto de datos numéricos y categóricos
- Manejo de valores especiales (TBD, N/A, nan)
- Imputación inteligente de scores y ratings

### 📊 **Preparación para Análisis**

- EDA automatizado para understanding rápido
- Preparación de datos para machine learning
- Limpieza para dashboards y visualizaciones
- Data quality checks automatizados

### ⚡ **Procesamiento de Datos Grandes**

- Optimización de memoria para datasets grandes
- Processing distribuido-ready architecture
- Batch processing para eficiencia
- Configuración flexible para diferentes escenarios

## 🎯 Configuración Ejemplo

### **YAML Configuration Completa**

```yaml 
Paths: 
  input_file: "video_games.csv"
  output_file: "games_cleaned.parquet"

Cleaning_Rules: 
  column_rename: 
    Name: 'name'
    Platform: 'platform'
    Year_of_Release: 'year_of_release'
    Genre: 'genre'
    
  dtype_override: 
    year_of_release: 'Int32'
    na_sales: 'Float32'
    eu_sales: 'Float32'
    
  null_values: 
    null_val_csv: ['tbd', 'TBD', 'N/A', 'nan']
    null_imput_num_operation: 'median'
    null_imput_cat_operation: 'mode'
```

## 🤝 Contribución

¡Las contribuciones son bienvenidas! Si tienes ideas para mejorar el pipeline:

1. Fork el proyecto
2. Crea una rama para tu feature (git checkout -b feature/mejora-limpieza)
3. Commit tus cambios (git commit -m 'Agregar nueva estrategia de imputación')
4. Push a la rama (git push origin feature/mejora-limpieza)
5. Abre un Pull Request

## 👩‍💻 Sobre Este Pipeline

Este sistema representa un approach profesional a la limpieza de datos, combinando optimización de rendimiento con robustez en la validación. Está diseñado para ser utilizado en entornos production donde la calidad y consistencia de los datos son críticas.

**¿Preguntas o sugerencias?** ¡No dudes en abrir un issue!
