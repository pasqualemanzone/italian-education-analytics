# 🎓 Italian Education System Analytics

> End-to-end data analysis project on the Italian school system using official Ministry of Education data.

---

## 📌 Overview

This project analyzes the Italian education system through multiple official datasets covering **360,000+ records** on school personnel, student enrollment, and EU funding allocation across all Italian provinces.

The goal is to uncover patterns in **gender distribution**, **workforce aging**, **regional disparities**, and **EU fund utilization** across Italian schools.

---

## 🔍 Key Findings

- **82%** of Italian teachers are female, with significant variation by school level
- **42%** of teachers are over 54 years old, indicating a critical workforce aging issue
- **2,645 schools** present in 2023/24 student data were missing from the 2025/26 registry, reflecting ongoing school consolidation policies in Italy
- Significant **North-South disparity** in both teacher distribution and EU fund absorption

---

## 🏗️ Architecture

```
Raw CSV Files (Official Ministry of Education Data)
        ↓
Python ETL Pipeline (Pandas + pyodbc)
        ↓
SQL Server Database (Star Schema)
        ↓
Power BI Dashboard (Interactive Analysis)
```

---

## 🗄️ Database Schema (Star Schema)

```
           dim_tempo
               │
    dim_province─┼──→ fact_personale
               │
           dim_scuole──→ fact_alunni
```

### Dimension Tables:
| Table | Description | Rows |
|-------|-------------|------|
| `dim_tempo` | School years | ~2 |
| `dim_province` | Italian provinces + regions | ~110 |
| `dim_scuole` | School registry (with inactive flag) | ~53,000 |

### Fact Tables:
| Table | Description | Rows |
|-------|-------------|------|
| `fact_personale` | Teaching staff by province/age/gender | ~3,300 |
| `fact_alunni` | Student enrollment by school/grade | ~305,000 |

---

## ⚠️ Data Quality Challenge

During exploratory analysis, **2,645 schools** (7% of students) present in the 2023/24 enrollment data were not found in the 2025/26 school registry.

**Root Cause:** Ongoing school consolidation policy in Italy — smaller schools being merged or closed between academic years.

**Solution:** Created placeholder records in `dim_scuole` with `scuola_attiva_2025 = 'NO'` flag, preserving 100% of historical student data while maintaining referential integrity.

---

## 🛠️ Tech Stack

| Tool | Usage |
|------|-------|
| Python (Pandas, pyodbc) | Data exploration, cleaning, ETL pipeline |
| SQL Server (T-SQL) | Star Schema database, computed columns, views |
| Power BI | Interactive dashboard and visualizations |
| SSMS | Database management and query testing |

---

## 📁 Repository Structure

```
italian-education-analytics/
├── README.md
├── sql/
│   └── schema_5_tabelle.sql        # Star Schema DDL (T-SQL)
├── python/
│   ├── ETL_personale2024.ipynb     # EDA and data cleaning
│   └── etl_carica_dati.py          # ETL pipeline script
└── powerbi/
    └── screenshots/                # Dashboard preview images
```

---

## 🚀 How to Run

### Prerequisites
- SQL Server (Express or higher)
- Python 3.x with `pandas` and `pyodbc`
- Power BI Desktop

### Steps

**1. Create the database:**
```sql
-- Run in SSMS
CREATE DATABASE ItalianEducationDB;
GO
-- Then execute: sql/schema_5_tabelle.sql
```

**2. Install Python dependencies:**
```bash
pip install pandas pyodbc
```

**3. Update the connection string in the ETL script:**
```python
SERVER = r'YOUR_SERVER\SQLEXPRESS'
CSV_PATH = r'your/path/to/csv/files'
```

**4. Run the ETL pipeline:**
```bash
python python/etl_carica_dati.py
```

**5. Connect Power BI:**
- Open Power BI Desktop
- Get Data → SQL Server
- Server: `YOUR_SERVER\SQLEXPRESS`
- Database: `ItalianEducationDB`

---

## 📊 Dashboard Preview

*Screenshots coming soon*

---

## 📂 Data Sources

All data is publicly available from the Italian Ministry of Education (MIUR):
- `Personale_2024.csv` — Teaching staff by province, school level, age group
- `Alunni_2024.csv` — Student enrollment by school and grade
- `Anagrafica_Scuola_202526.csv` — Official school registry

---

## 👤 Author

**Pasquale Manzone**  
Data Analyst | Brussels, Belgium  
---

## 📄 License

This project uses publicly available data from the Italian Ministry of Education.  
Code is available under the MIT License.
