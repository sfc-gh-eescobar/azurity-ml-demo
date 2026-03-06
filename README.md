# Azurity Pharmaceuticals ML and Spark Connect Demos

End-to-end machine learning demo showcasing Snowflake's ML capabilities for pharmaceutical demand forecasting.

## Business Context

**Azurity Pharmaceuticals** specializes in pediatric medications and oral solutions. This demo predicts prescription volume to optimize:
- Inventory management
- Sales rep targeting (HCP prioritization)
- Campaign effectiveness (A/B testing)

## Snowflake Capabilities Demonstrated

| Capability | Description | Notebook |
|------------|-------------|----------|
| **Feature Store** | Centralized ML features with point-in-time correctness | `azurity_prescription_forecasting.ipynb` |
| **Model Registry** | Version models with metrics, enable SQL inference | `azurity_prescription_forecasting.ipynb` |
| **Experiment Tracking** | Compare hyperparameters and model versions in Snowsight UI | `azurity_prescription_forecasting.ipynb` |
| **Streamlit in Notebooks** | Interactive visualizations vs. static PowerBI | `azurity_prescription_forecasting.ipynb` |
| **Spark Connect** | Run PySpark code against Snowflake (no data movement) | `azurity_spark_connect_analytics.ipynb` |
| **Container Runtime** | pip install packages, consistent environments | Both notebooks |
| **Git Integration** | Snowflake Workspaces with GitHub sync | All files |

## Setup Instructions

### 1. Run Infrastructure Setup

Execute `setup_azurity_demo.sql` with ACCOUNTADMIN to create:
- Database: `AZURITY_DEMO_DB`
- Schemas: `ML`, `RAW`
- Compute Pool: `AZURITY_ML_POOL`
- External Access: `AZURITY_PYPI_EAI`
- Synthetic data tables

```sql
-- Run in Snowsight SQL Worksheet
!source setup_azurity_demo.sql
```

### 2. Create Git Repository in Snowflake

```sql
-- If not already configured
CREATE OR REPLACE API INTEGRATION GITHUB_API_INTEGRATION
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-eescobar/')
    ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY AZURITY_DEMO_DB.PUBLIC.AZURITY_ML_REPO
    API_INTEGRATION = GITHUB_API_INTEGRATION
    ORIGIN = 'https://github.com/sfc-gh-eescobar/azurity-ml-demo';
```

### 3. Run Notebooks in Snowflake Workspaces

1. Navigate to **Projects > Notebooks** in Snowsight
2. Create notebook from Git repository
3. Select **Container Runtime** with compute pool `AZURITY_ML_POOL`
4. Enable External Access Integration `AZURITY_PYPI_EAI`

## Notebook Guide

### Prescription Forecasting (`azurity_prescription_forecasting.ipynb`)

**Runtime**: Container Runtime with `AZURITY_ML_POOL`

This notebook demonstrates a complete ML workflow for predicting 30-day prescription volume by product and healthcare provider.

Key sections:
1. **Data Exploration** - Azurity product portfolio (Eprontia, Katerzia, Qbrelis, etc.) and HCP distribution across 10,000 providers
2. **Feature Store Setup** - Register entities (PRODUCT, HCP) and create feature views with automatic refresh
3. **Training Data Generation** - Point-in-time correct feature retrieval using ASOF joins to prevent data leakage
4. **Model Training** - XGBoost regressor predicting 30-day prescription volume
5. **Model Registry** - Register model V1 with metrics, enable SQL inference via `MODEL!PREDICT()`
6. **Experiment Tracking** - Train model V2 with tuned hyperparameters, compare versions in Snowsight (AI & ML → Experiments)
7. **Streamlit A/B Test Analysis** - Interactive campaign comparison with statistical significance testing

**What to demo**:
- Show how Feature Store prevents accidental data leakage in backtesting
- Run SQL inference directly from a worksheet using `AZURITY_RX_FORECASTER!PREDICT()`
- Navigate to AI & ML → Experiments to compare V1 vs V2 model performance
- Interactive Streamlit charts embedded in the notebook

### Spark Connect Analytics (`azurity_spark_connect_analytics.ipynb`)

**Runtime**: Container Runtime with `AZURITY_ML_POOL` (Python 3.11 required)

This notebook demonstrates running PySpark code against Snowflake data without copying data out. Ideal for teams migrating from Databricks or with existing Spark codebases.

Key sections:
1. **Spark Connect Setup** - Install `snowpark-connect[jdk]` and `jdk4py` for JVM in Container Runtime
2. **DataFrame Operations** - PySpark groupBy, join, filter operations executed as Snowflake SQL
3. **Window Functions** - Running totals, rankings, and lag calculations
4. **Data Enrichment** - Join prescriptions with HCP data for regional analysis
5. **Write Back** - Save Spark DataFrames as Snowflake tables with `.write.mode("overwrite").saveAsTable()`

**What to demo**:
- Existing PySpark code runs unchanged against Snowflake
- All operations push down to Snowflake SQL (no data movement)
- Show the Spark UI to demonstrate query execution
- Compare identical PySpark syntax with native Snowpark Python

## Key Talking Points

### vs. Databricks

| Aspect | Snowflake | Databricks |
|--------|-----------|------------|
| Environment | Single platform (SQL + ML + Apps) | Separate from warehouse |
| Governance | Unified RBAC for data + models | Multiple systems |
| SQL Inference | Native `MODEL!PREDICT()` | Requires MLflow serving |
| Spark Support | Snowpark Connect (no data movement) | Native but data copies |

### Feature Store Benefits

- **Point-in-time correctness**: ASOF JOIN prevents data leakage in backtesting
- **Auto-refresh**: Features update as source tables change
- **Lineage**: Track feature usage across models

### Container Runtime Benefits

- **pip install**: Use any PyPI package
- **Consistent environments**: Same packages in dev and prod
- **GPU support**: Available for deep learning workloads

## Synthetic Data Schema

```
RAW.PRODUCTS
- PRODUCT_ID, PRODUCT_NAME, GENERIC_NAME, THERAPEUTIC_AREA
- Azurity portfolio: Eprontia, Katerzia, Qbrelis, Zonisade, Arynta

RAW.HEALTHCARE_PROVIDERS (10,000 HCPs)
- HCP_ID, SPECIALTY, DECILE (1-10), STATE, REGION

RAW.PRESCRIPTIONS (2+ years)
- RX_ID, PRODUCT_ID, HCP_ID, FILL_DATE, QUANTITY, PAYER_TYPE

RAW.EMAIL_CAMPAIGNS
- CAMPAIGN_ID, PRODUCT_ID, HCP_ID, VARIANT (A/B), SENT_DATE, OPEN_DATE, CLICK_DATE
```

## Model Details

**Model**: `AZURITY_DEMO_DB.ML.AZURITY_RX_FORECASTER`

| Version | Parameters | Use Case |
|---------|------------|----------|
| V1 | n_estimators=100, max_depth=6, learning_rate=0.1 | Baseline model |
| V2 | n_estimators=200, max_depth=8, learning_rate=0.05, regularization | Tuned for better accuracy |

| Parameter | Value |
|-----------|-------|
| Algorithm | XGBoost Regressor |
| Target | 30-day prescription volume |
| Features | RX_VOLUME_2W, RX_VOLUME_4W, RX_AVG_LAST_WEEK, RX_COUNT_4W |

**Experiment**: `AZURITY_RX_FORECASTING_EXPERIMENT`
- View in Snowsight: AI & ML → Experiments
- Compare V1 vs V2 metrics (RMSE, MAE, R²)
- Track hyperparameter differences

SQL Inference:
```sql
SELECT 
    HCP_ID,
    AZURITY_RX_FORECASTER!PREDICT(...) AS PREDICTED_VOLUME
FROM scoring_input;
```

## Git Workflow Demo

1. **Clone repo** in Snowflake Workspaces
2. **Edit notebook** in Snowsight
3. **Commit changes** back to GitHub
4. **Pull updates** to sync with team

## Contact

Demo created for Azurity Pharmaceuticals by Snowflake SE team.
