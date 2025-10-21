# NFL Sports Analytics Platform

A comprehensive sports analytics platform built with modern data engineering practices, featuring predictive modeling, real-time analytics, and interactive dashboards for NFL data analysis.

## Architecture

This platform follows a **Modern Data Stack** approach:

- **Data Ingestion**: Python scripts with Polars for efficient data processing
- **Data Warehouse**: Google BigQuery for scalable analytics workloads
- **Transformation Layer**: dbt for data modeling and transformations
- **Application Database**: PostgreSQL for application state and metadata
- **ML/Analytics**: Python with scikit-learn and XGBoost for predictive modeling
- **Orchestration**: Prefect for workflow management
- **Frontend**: Streamlit for rapid prototyping + Next.js for production UI
- **Backend API**: FastAPI for serving predictions and analytics

## Features

### Current (NFL Focus)
- **Player Analytics**: Historical performance metrics, trends, and insights
- **Game Predictions**: Win/loss probabilities, total points forecasting
- **Interactive Dashboards**: Real-time data visualization with Streamlit
- **ML Pipeline**: Automated model training and evaluation

### Planned Expansions
- **College Football (CFB)**: Extend analytics to NCAA Football
- **NBA**: Basketball analytics and predictions
- **Cross-Sport Analysis**: Unified metrics across different sports

## Tech Stack

### Core Technologies
- **Python 3.11+** with **uv** for package management
- **Polars** for high-performance data manipulation
- **Google BigQuery** for cloud data warehouse
- **dbt** for data transformations
- **PostgreSQL** for application database
- **FastAPI** for REST API development
- **Streamlit** for interactive dashboards

### ML & Analytics
- **scikit-learn** and **XGBoost** for machine learning
- **MLflow** for experiment tracking and model management
- **Prefect** for workflow orchestration

### Infrastructure
- **Google Cloud Platform** for cloud infrastructure
- **Terraform** for Infrastructure as Code
- **Docker** for containerization
- **Kubernetes** for orchestration (future)

## Project Structure

```
nfl_v3/
├── data/                    # Local data storage
├── ingestion/              # Data ingestion layer
├── dbt_project/           # dbt transformation models
├── ml/                    # Machine learning models
├── api/                   # FastAPI backend
├── streamlit_app/         # Streamlit dashboards
├── orchestration/         # Workflow orchestration
├── infrastructure/        # IaC and deployment configs
└── tests/                 # Test suites
```

## Quick Start

### Prerequisites
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- Google Cloud Platform account
- Docker (optional, for containerized development)

### Setup

1. **Clone and setup environment**:
   ```bash
   git clone <repository-url>
   cd nfl_v3
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. **Install dependencies with uv**:
   ```bash
   uv sync
   ```

3. **Setup GCP and BigQuery**:
   ```bash
   cd infrastructure/terraform
   terraform init
   terraform plan
   terraform apply
   ```

4. **Initialize dbt project**:
   ```bash
   cd dbt_project
   cp profiles.yml.example profiles.yml
   # Edit profiles.yml with your BigQuery credentials
   dbt deps
   dbt run
   ```

5. **Run the Streamlit app**:
   ```bash
   streamlit run streamlit_app/Home.py
   ```

6. **Start the API server**:
   ```bash
   cd api
   uvicorn main:app --reload
   ```

## Data Sources

- **NFL API**: Official NFL statistics and game data
- **ESPN API**: Additional sports data and analytics
- **Future**: College Football and NBA data sources

## Development

### Code Quality
- **ruff** for linting and formatting
- **mypy** for type checking
- **pytest** for testing
- **pre-commit** hooks for automated quality checks

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## Portfolio Highlights

This project demonstrates:
- Modern data engineering practices
- Cloud data warehouse expertise (BigQuery)
- Data transformation with dbt
- ML pipeline development and productionization
- API development for serving analytics
- Interactive data visualization
- Infrastructure as Code
- Testing and code quality practices

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or support, please open an issue or contact [your-email@example.com].
