# Azure End-to-End ETL Pipeline

![Deploy](https://github.com/ziyaratmahmudzade/azetlpipeline/actions/workflows/deploy.yml/badge.svg)
![Azure](https://img.shields.io/badge/Azure-0078D4?logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/PowerBI-F2C811?logo=powerbi&logoColor=black)

A fully automated, production-grade ETL pipeline on Microsoft Azure. Ingests four source files daily, transforms them through a 3-layer Medallion Architecture, and delivers clean business data to Power BI dashboards — zero manual intervention.

---

## Architecture
CSV / XLSX / JSON → Azure Data Factory → ADLS Gen2 (raw/)
↓
Azure Databricks
nb_clean.py
↓
ADLS Gen2 (cleaned/)
↓
Azure Databricks
nb_transform.py
↓
ADLS Gen2 (curated/)
↓
Synapse Serverless SQL views
↓
Power BI · 6 dashboards

### Medallion Architecture — Bronze → Silver → Gold

| Layer | Zone | Folder | What happens |
|---|---|---|---|
| Bronze | Raw | `raw/` | Original files land here unchanged via ADF |
| Silver | Clean | `clean/` | Cleaned, validated, typed correctly |
| Gold | Curated | `curated/` | Joined, enriched, business-ready Delta tables |

---

## Technology Stack

| Layer | Service |
|---|---|
| Orchestration | Azure Data Factory |
| Storage | ADLS Gen2 + Delta Lake |
| Transformation | Azure Databricks (PySpark) |
| Serving | Azure Synapse Analytics (Serverless) |
| Visualisation | Power BI |
| Security | Azure Key Vault |
| CI/CD | GitHub Actions |
| Monitoring | Azure Monitor |

---

## Repository Structure
azetlpipeline/
│
├── .github/
│ └── workflows/
│ └── deploy.yml # CI/CD — auto-deploys notebooks on push to main
│
├── adf/ # Azure Data Factory definitions
│ ├── dataset/ # Source and sink dataset JSON
│ ├── linkedService/ # ADLS and Databricks linked services
│ ├── pipeline/ # Pipeline JSON definitions
│ └── trigger/ # Schedule trigger definitions
│
├── notebooks/ # Databricks PySpark notebooks
│ ├── nb_ingest.py # Reads all 4 source files from Bronze zone
│ ├── nb_clean.py # Bronze → Silver: cleans and validates
│ ├── nb_tranform.py # Silver → Gold: joins, enriches, writes
│ ├── nb_validate.py # 19 automated data quality checks on Gold
│ └── nb_mount_adls.py # ADLS Gen2 mount utility
│
├── synapse/ # Synapse Analytics SQL scripts
│ └── create_views.sql # Creates 5 views over Gold Delta tables
│
├── data/
│ └── sample test data/ # Synthetic sample data files
│ ├── sales_transactions.csv # 900 rows, 18 columns
│ ├── master_data.xlsx # 100 customers + 20 products
│ ├── support_tickets.json # 850 records, 16 fields
│ └── marketing_campaigns.csv # 820 rows, 17 columns
│
├── tests/ # Unit tests
├── .gitignore # Excludes sensitive data files
└── README.md # This file

---

## Data Sources

| File | Format | Rows | Columns |
|---|---|---|---|
| `sales_transactions.csv` | CSV | 900 | 18 |
| `master_data.xlsx` | XLSX | 100 + 20 | 17 / 15 |
| `support_tickets.json` | JSON | 850 | 16 |
| `marketing_campaigns.csv` | CSV | 820 | 17 |

---

## Delta Tables by Layer

### Silver — `cleaned/`
| Table | Rows | Description |
|---|---|---|
| `transactions` | ~701 | Cleaned sales — negatives, nulls, cancellations removed |
| `customers` | ~81 | Active customers only — typed and standardised |
| `products` | 20 | Products with calculated margin percentage |
| `support` | 850 | Tickets with typed dates and standardised severity |
| `marketing` | 820 | Campaigns with calculated ROI percentage |

### Gold — `curated/`
| Table | Description |
|---|---|
| `transactions` | Enriched with customer segment, total spend, joined to master data |
| `customers` | Business-ready customer profiles |
| `products` | Product catalogue with margin metrics |
| `support` | Support analytics ready for Power BI |
| `marketing` | Campaign performance ready for Power BI |

---

## CI/CD

Every push to `main` triggers GitHub Actions which deploys all notebooks in `notebooks/` to `/Shared/etl-pipeline/` in Databricks automatically.

**Required secrets:**

| Secret | Description |
|---|---|
| `DATABRICKS_HOST` | Databricks workspace URL |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `AZURE_CREDENTIALS` | Service principal JSON |

---

## Power BI Dashboard

6 pages covering the full business picture:

| Page | Key insights |
|---|---|
| Sales Overview | Revenue by region, channel, trend over time |
| Customer Intelligence | Segments, industries, countries, credit limits |
| Product Performance | Margins, stock levels, supplier revenue |
| Support Analytics | Ticket severity, resolution time, satisfaction scores |
| Marketing Performance | ROI by campaign type, leads by platform |
| Executive Summary | CEO-level KPIs with region, date, segment slicers |

Scheduled refresh daily at 07:00 UTC via Synapse Serverless SQL.

---

## Setup

### Prerequisites
- Azure subscription (Owner or Contributor role)
- GitHub account
- Power BI Desktop

### 1. Azure Infrastructure

| Resource | Name | Notes |
|---|---|---|
| Resource Group | `rg-etl-pipeline` | All resources in same region |
| Storage Account | `saetlpipeline` | Hierarchical namespace ON |
| Key Vault | `kv-etl-pipeline` | Store `adls-storage-key` secret |
| Data Factory | `adf-etl-pipeline` | Connect to this GitHub repo |
| Databricks | `dbw-etl-pipeline` | Runtime 13.3 LTS, link Key Vault as `kv-scope` |
| Synapse Analytics | `synw-etl-pipeline` | Linked to storage account |

### 2. Permissions

- Grant `Storage Blob Data Contributor` to ADF and Databricks managed identities
- Grant `Storage Blob Data Reader` to Synapse managed identity

### 3. Run the Pipeline
1. Upload source files to ADLS raw zone
2. Run ADF pipeline pl_ingest_files
3. Run notebooks in order:
nb_mount_adls.py → nb_ingest.py → nb_clean.py → nb_transform.py → nb_validate.py
4. Run synapse/curated_zone.sql then the cz_vw... in series and select_curated_zone_views.sql for verification in Synapse Studio
5. Connect Power BI to Synapse Serverless endpoint

### 4. Monitoring
Create Azure Monitor alert on ADF — condition `Failed pipeline runs > 0` — with email notification via Action Group.

---

## License

MIT