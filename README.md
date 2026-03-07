# 🧹 Data Cleaning Pipeline

[![Polars](https://img.shields.io/badge/Polars-Fast__DataFrames-red.svg)](https://pola.rs/)
[![Prefect](https://img.shields.io/badge/Prefect-Workflow__Orchestration-blue.svg)](https://www.prefect.io/)
[![Pydantic](https://img.shields.io/badge/Pydantic-Data__Validation-green.svg)](https://pydantic-docs.helpmanual.io/)
[![Python](https://img.shields.io/badge/Python-3.8+-yellow.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A comprehensive, automated data cleansing system that integrates exploratory analysis, intelligent memory management, and robust preprocessing pipelines with data validation.

## 📚 Professional Data Cleaning System

This repository contains a data cleaning pipeline designed to handle large datasets with efficiency, robust validation, and optimized processing.

## 🏗️ Repository Structure

### 🔍 **01 - Exploratory Analysis**

| Module       | Technologies     | Functionalities        |
|--------------|-----------------|------------------------------------------------|
| **EDA.py** | Polars, Logging | Automatic analysis, statistics, null detection, preview |

### 💾 **02 - Intelligent Memory Management**  

| Module            | Technologies              | Features                                         |
|-------------------|--------------------------|----------------------------------- |
| **GetFrame.py** | Polars, Psutil, Pydantic | Automatic Eager/Lazy decision, memory optimization |

### 🚀 **03 - Cleaning Pipeline**

| Module                     | Technologies               | Features                                             |
|----------------------------|---------------------------|--------------------------------------------------------|
| **DataPreProcessing.py** | Polars, Prefect, Pydantic | Renaming, typing, null handling, intelligent imputation |

### ⚙️ **04 - Configuration and Validation**

| Module                                 | Technologies          | Features                                 |
|----------------------------------------|----------------------|-------------------------------------------------|
| **ReadFile.py** **ValidatorConfig.py** | Pydantic, YAML, TOML | Configuration validation, data schemas |

### 🎯 **05 - Strategies and Patterns**

| Module              | Technologies | Functionalities                                        |
|---------------------|-------------|--------------------------------------------------------|
| **Strategies.py** | Enum        | Imputation strategies, data types, operations |

## 🎯 Key Features

### 🧠 **Intelligent Memory Management**

```python
def decision_frame(self) -> str: 
    file_size = self.file.stat().st_size
    available_memory = psutil.virtual_memory().available
    
    # Automatic Eager vs Lazy decision based on resources
    if file_size < available_memory * 0.1: 
        return 'eager'
    elif file_size < available_memory * 0.75: 
        return 'lazy'
```

### 🔧 **Advanced Null Value Handling**

```python 
def pipeline_null_handler(self, representative_columns: List[str]) -> pl.LazyFrame:
    # Hierarchical analysis of nulls by representative columns
    new_frame = AnalysisNullData(
        frame=frame, 
        model=self.model, 
        representative_columns=representative_columns, 
        target_columns=self.analyse_null
    )
    return new_frame.analysis_null_data()
```

### 📊 **Automated Exploratory Analysis**

```python 
def BasicEDA(self) -> None: 
    self.first_view()        # First 10 rows
    self.general_info()         # Dataset metadata
    self.general_statistics()  # Descriptive statistics
    self.missing_values()        # Analysis of missing values
```

## 🚀 Implementation Examples

### 🔍 **Quick Exploratory Analysis**

```python 
# Complete analysis with one line
eda = EDAGeneral(frame=df_lazy)
eda.EDA_Basico()

# Automatic output:
# - First 10 rows
# - Shape and data types
# - Descriptive statistics
# - Null value analysis
```

### 🧹 **Complete Cleaning Pipeline**

```python 
# Automated pipeline with Prefect
cleaning_pipeline = DataCleaning(frame=df, model=config_model)

df_clean = cleaning_pipeline.pipeline_data_cleaning(
    representative_columns=['category', 'group'],
    threshold=0.4,
    min_proportion=45)

```

### ⚡ **Configuration with Validation**

```yaml
Paths: 
  input_file: "data.csv"
  output_file: "clean_data.parquet"

Cleaning_Rules: 
  column_rename: {'Name':'name', 'Platform':'platform'}
  dtype_override: {'year_of_release': 'Int32', 'sales': 'Float32'}
  null_values: 
    null_val_csv: ['tbd', 'TBD', 'N/A', 'nan']
    null_imput_num_operation: 'median'
    null_imput_cat_operation: 'mode'
```

## 📦 Installation and Use

### **Requirements**

```bash 
# requirements.txt
polars>=0.19.0
prefect>=2.0.0
pydantic>=2.0.0
pyyaml>=6.0
tomli>=2.0.0
psutil>=5.9.0
```

### **Quick Use**

```bash 
# Clone the repository
git clone https://github.com/sm7ss/data-cleaning-pipeline.git
cd data-cleaning-pipeline

# Install dependencies
pip install -r requirements.txt

# Run full cleaning
python -c "
from ReadFile import ReadConfig
from GetFrame import FrameCollector
from DataPreProcessing import DataCleaning

config = ReadConfig('config.yaml').read_config()
frame = FrameCollector(model=config).get_frame()
cleaning = DataCleaning(frame=frame, model=config)
result = cleaning.pipeline_data_cleaning(['category'])
print('Cleaning complete!')
"
```

## 🛠 Architecture and Technologies

| Technology     | Version | Purpose                         |
|----------------|---------|-----------------------------------|
| **Polars**     | 0.19+   | Fast data processing     |
| **Prefect**     | 2.0+       | Pipeline orchestration        |
| **Pydantic**     | 2.0+       | Configuration validation     |
| **Psutil**     | 5.9+       | System resource monitoring |
| **Python**     | 3.8+       | Base language with type hints      |

## 🔍 Advanced Features

### **1. Intelligent Null Analysis**

- Automatic detection of missing value patterns
- Strategic imputation based on representative columns
- Configurable thresholds for deletion vs. imputation
- Hierarchical analysis by data groups

### **2. Performance Optimization**

- Automatic Eager vs. Lazy decision
- Memory management based on system resources
- Batch processing for large datasets
- Streaming processing for optimization

### **3. Robust Validation**

- Data schemas with Pydantic
- YAML/TOML configuration validation
- Column and data type verification
- Professional error handling with logging

## 📈 Implemented Use Cases

### 🧹 **Game Data Cleaning**

- Column name normalization
- Correct typing of numerical and categorical data
- Handling of special values (TBD, N/A, nan)
- Intelligent imputation of scores and ratings

### 📊 **Preparation for Analysis**

- Automated EDA for quick understanding
- Data preparation for machine learning
- Cleaning for dashboards and visualizations
- Automated data quality checks

### ⚡ **Big Data Processing**

- Memory optimization for large datasets
- Distributed processing-ready architecture
- Batch processing for efficiency
- Flexible configuration for different scenarios

## 🎯 Example Configuration

### **Complete YAML Configuration**

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

## 🤝 Contribution

Contributions are welcome! If you have ideas to improve the pipeline:

1. Fork the project
2. Create a branch for your feature (git checkout -b feature/improve-cleanup)
3. Commit your changes (git commit -m 'Add new imputation strategy')
4. Push to the branch (git push origin feature/improve-cleanup)
5. Open a Pull Request

## 👩‍💻 About This Pipeline

This system represents a professional approach to data cleaning, combining performance optimization with robust validation. It is designed for use in production environments where data quality and consistency are critical.

**Questions or suggestions?** Feel free to open an issue!
