# Azurity Pharmaceuticals ML Demo

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

Key sections:
1. **Data Exploration** - Azurity product portfolio and HCP distribution
2. **Feature Store Setup** - Register entities and feature views
3. **Training Data Generation** - Point-in-time correct features
4. **Model Training** - XGBoost regressor for 30-day volume
5. **Model Registry** - Register with metrics, enable SQL inference
6. **Streamlit A/B Test Analysis** - Interactive campaign comparison

### Spark Connect Analytics (`azurity_spark_connect_analytics.ipynb`)

**Runtime**: Container Runtime with `AZURITY_ML_POOL`

Key sections:
1. **Spark Connect Setup** - jdk4py for JVM in Container Runtime
2. **PySpark Operations** - groupBy, join, window functions
3. **SQL Pushdown** - Operations translated to Snowflake SQL
4. **Write Back** - Save Spark results as Snowflake tables

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

| Parameter | Value |
|-----------|-------|
| Algorithm | XGBoost Regressor |
| Target | 30-day prescription volume |
| Features | RX_VOLUME_2W, RX_VOLUME_4W, RX_AVG_LAST_WEEK, RX_COUNT_4W |

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
