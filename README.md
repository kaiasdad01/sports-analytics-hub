# Sports Analytics Platform

A comprehensive sports analytics platform, starting with NFL, to analyze team and player data - and make predictions about future outcomes. 

## Architecture

This platform follows a **Modern Data Stack** approach:

- **Data Ingestion**: Python scripts with Polars
- **Data Warehouse**: Google BigQuery 
- **Transformation Layer**: dbt for data modeling and transformations
- **Application Database**: PostgreSQL for application state and metadata
- **ML/Analytics**: Python with scikit-learn and XGBoost for predictive modeling
- **Orchestration**: Prefect (to be added)
- **Frontend**: Streamlit for initial interaction layer -- eventually full deployment
- **Backend API**: FastAPI for serving predictions and analytics

## Features

### Current (NFL Focus)
- **Player Analytics**: Historical performance metrics, trends, and insights
- **Game Predictions**: Win/loss probabilities, total points forecasting
- **Player Predictions**: QB Passing Yards, Rushing Yards, TDs, etc. 
- **Interactive Dashboards**: Real-time data visualization with Streamlit (and maybe Looker?)
- **ML Pipeline**: Automated model training and evaluation; implement MLFlow

### Planned Expansions - near term
- **College Football (CFB)**: Extend analytics to NCAA Football using CFBdata API | P1
- **NBA**: Basketball analytics and predictions | P2


## Tech Stack

### Core Technologies
- **Python+** with **uv**
- **Polars** (though Pandas still used in some spots)
- **Google BigQuery** 
- **dbt** 
- **PostgreSQL**
- **FastAPI** (for getting predictions)
- **Streamlit** 

### Other considerations 
- **Looker** for viz

### ML & Analytics
- **scikit-learn** and **XGBoost** for machine learning
- **MLflow** for experiment tracking and model management
- **Prefect** for workflow orchestration

### Infrastructure
- **Google Cloud Platform** for cloud infrastructure
- **Terraform** for Infrastructure as Code
- **Docker** for containerization


## Project Structure

```
nfl_v3/
├── data/                  # Local data storage
├── ingestion/             # Data ingestion layer
├── dbt_project/           # dbt transformation models
├── ml/                    # Machine learning models
├── api/                   # FastAPI backend
├── streamlit_app/         # Streamlit dashboards
├── orchestration/         # Workflow orchestration
├── infrastructure/        # IaC and deployment configs
└── tests/                 # Test suites
```

## Data Sources

- **nflreadypy** | for most core API data
- **ftn** | for enhanced fantasy data (not yet connected)
- **ESPN API** | for real-time scores and data (not yet connected)

To be confirmed: 
- Odds data source
