
## Overview

This project demonstrates a complete **real-time data pipeline** built with Dagster, designed to fetch, process, validate, and analyze conflict event data from the ACLED (Armed Conflict Location & Event Data Project) API. The system showcases end-to-end data engineering capabilities from ingestion to machine learning-powered insights.

### What is Dagster?

Dagster is a data orchestration platform that helps teams **organize, run, and monitor data processes**. Think of it as a **project manager for data pipelines**—it ensures data tasks execute in the correct order, reliably, with full visibility into the process.

**Key Benefits:**
- **Reliability** – Ensures workflows run correctly and on schedule, reducing errors
- **Transparency** – Provides clear visibility into data lineage, processing status and customizable metadata.
- **Efficiency** – Automates repetitive tasks, saving time and reducing manual effort
- **Flexibility** – Adapts as requirements change, enabling smooth integration of new processes
- **Collaboration** – Facilitates teamwork between engineers, analysts, and stakeholders

## Architecture Overview

```
ACLED API → Data Ingestion → S3 Storage → Data Validation → PostgreSQL → ML Pipeline → Reports & Analytics
    ↑             ↑                           ↑
  Sensors    Asset Checks              Real-time Monitoring
```

<img width="1253" height="301" alt="image" src="https://github.com/user-attachments/assets/78cf3788-8931-4b72-b21b-6639933a7c78" />


## Core Components

### 1. **Real-Time Data Ingestion**
- Automated daily fetching from ACLED API endpoints
- Configurable parameters for flexible data queries
- Async HTTP processing for optimal performance
- Sensor-based triggering when new data is available

### 2. **Data Quality & Validation**
- Comprehensive asset checks ensuring data integrity
- Automated validation of coordinates, event types, and required fields
- Data completeness monitoring and quality scoring
- Partition-level validation to catch issues early

### 3. **Scalable Storage Architecture**
- **S3**: Cost-effective data lake for raw event data
- **PostgreSQL**: Structured storage for analytics and ML training
- Custom IO managers for seamless data movement
- Partitioned storage for efficient querying

### 4. **Machine Learning Pipeline**
- **Predictive modeling** for fatality forecasting using XGBoost
- Feature engineering with target encoding and geographic analysis
- Hyperparameter tuning with cross-validation
- Model performance tracking and validation

### 5. **Automated Intelligence Reports**
- Multi-page PDF reports with visualizations
- ML-enhanced forecasting reports with risk assessments

### 6. **Monitoring & Observability**
- Real-time pipeline health monitoring
- Detailed execution metadata and performance metrics
- Asset lineage tracking and dependency management
- Comprehensive logging and error handling

## Business Intelligence Reports
The platform automatically generates professional, multi-page intelligence reports combining data quality metrics, regional analysis, and ML-powered forecasts. These reports demonstrate automated stakeholder communication capabilities essential for production data systems.
<img width="1070" height="748" alt="image" src="https://github.com/user-attachments/assets/5d2bfe99-3f35-4b0f-a8dd-ea103635fb1c" />
<img width="1053" height="739" alt="image" src="https://github.com/user-attachments/assets/78c57314-c385-4ece-8f50-661db55d9bdc" />



## Professional Relevance

This project directly demonstrates capabilities relevant to modern data engineering roles:

**Real-Time Processing**: This project uses conflict data, however the architecture patterns are for most data engineering workloads are identical: automated data collection, real-time validation, and anomaly detection.

**AWS Integration**: Leverages core AWS services for seamless transition into a production environment.

**Analytics & ML**: Demonstrates ability to extract business insights from raw data streams and build predictive models for forecasting and anomaly detection.

**Stakeholder Engagement**: Includes data flow monitoring—essential for enterprise environments.

## Technical Stack

| Component | Technology |
|-----------|------------|
| **Orchestration** | Dagster |
| **Storage** | AWS S3 + AWS RDS PostgreSQL |
| **Secret Management** | AWS Secrets Manager | 
| **Processing** | Pandas, Numpy, Polars, Python |
| **ML** | Scikit-learn, XGBoost |
| **Visualization** | Matplotlib |

## Key Features

✅ **Automated Data Quality Monitoring**: Comprehensive validation ensuring data reliability  
✅ **Intelligent Partitioning**: Efficient data organization for scalable querying  
✅ **ML-Powered Forecasting**: Predictive models for risk assessment and trend analysis  
✅ **Professional Reporting**: Automated generation of stakeholder-ready intelligence reports  
✅ **Robust Error Handling**: Graceful failure management with detailed logging  
✅ **Scalable Architecture**: Designed for production deployment and monitoring  

## Quick Start

First, you will need to create an ACLED API key by following the instructions here https://acleddata.com/api-documentation/getting-started
Secondly, you will need an AWS account with the aforementioned services. 

```bash
# Install dependencies
pip install -e ".[dev]"

# Configure environment
cp .env.example .env
# Add your API keys and AWS credentials

# Start Dagster UI
dagster dev

# Access the pipeline at http://localhost:3000
```


*This project represents a comprehensive demonstration of modern data engineering practices, directly applicable to IoT analytics, real-time monitoring, and business intelligence platforms.*

## 📝 License

MIT License - feel free to use this project as a reference for your own data engineering work.
