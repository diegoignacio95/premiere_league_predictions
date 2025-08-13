# Football Predictions Project - Technical Documentation

## Project Overview

The **Football Predictions Project** is a comprehensive machine learning system designed to predict football match outcomes and goal differences using historical data from multiple football leagues. The project implements a full data science pipeline from web scraping to model deployment, with primary focus on the Premier League.

### Key Capabilities
- **Data Collection**: Automated web scraping of match statistics, fixtures, and team wages from FBRef.com
- **Data Engineering**: Advanced feature engineering with rolling averages, form metrics, and team comparisons
- **Machine Learning**: Regression and classification models for predicting match outcomes and goal differences
- **Production Ready**: Configurable environments (dev/prod/test) with comprehensive logging and error handling

## Technical Architecture

### Project Structure

```
Pronósticos Football/
├── config/                     # Configuration management
│   ├── dev.yaml               # Development environment settings
│   ├── prod.yaml              # Production environment settings
│   ├── testing.yaml           # Testing/validation settings
│   └── README.md              # Configuration documentation
├── data/                      # Data storage hierarchy
│   ├── dev/                   # Development environment data
│   │   ├── raw/               # Raw scraped data
│   │   ├── processed/         # Cleaned and processed data
│   │   └── external/          # External data sources
│   └── prod/                  # Production environment data
│       ├── raw/               # Production raw data
│       │   ├── match_stats/   # Match statistics data
│       │   └── ...
│       ├── processed/         # Production processed data
│       │   └── masters/       # Master datasets
│       └── external/          # External production data
├── notebooks/                 # Jupyter notebooks for analysis
│   ├── exploratory/           # Exploratory data analysis
│   │   ├── data_collection/   # Data collection experiments
│   │   ├── data_engineering/  # Data processing experiments
│   │   └── machine_learning/  # ML model development
│   ├── production/            # Production notebooks
│   └── evaluation/            # Model evaluation notebooks
├── scripts/                   # Production Python scripts
│   ├── data_collection/       # Data collection modules
│   ├── prediction/            # Prediction utilities
│   ├── training/              # Model training scripts
│   └── utils/                 # Utility functions
├── models/                    # Trained model artifacts
│   ├── premier_league/        # Premier League specific models
│   └── laliga/                # La Liga specific models
├── tests/                     # Test suites
│   ├── unit/                  # Unit tests
│   └── integration/           # Integration tests
├── results/                   # Analysis results and outputs
└── utils/                     # Shared utilities
```

### Data Pipeline Architecture

#### 1. Data Collection Layer
- **Web Scraping**: Beatifulsoup-based scraping of FBRef.com
- **Rate Limiting**: Configurable delays to respect website policies
- **Error Handling**: Robust retry mechanisms and progress tracking
- **Multi-format Output**: JSON, CSV, and Parquet support

#### 2. Data Processing Layer
- **Data Cleaning**: Missing value handling and data type conversions
- **Feature Engineering**: Rolling averages, form metrics, wage differentials
- **Master Dataset Creation**: Unified dataset combining all data sources
- **Validation**: Data quality checks and consistency validation

#### 3. Machine Learning Layer
- **Model Training**: XGBoost Random Forest Regressor for goal difference prediction
- **Hyperparameter Optimization**: Optuna-based optimization with time-series cross-validation
- **Model Evaluation**: Comprehensive regression and classification metrics
- **Model Persistence**: Joblib serialization for model artifacts

## Core Components

### Data Collection System

#### Team ID Mapping (`team_id_mapper.py`)
- Maps team names across different data sources
- Handles team name variations and historical changes
- Creates standardized team identifiers

#### Fixtures Collector (`fixtures_collector.py`)
- Scrapes match fixtures and results
- Extracts venue, date, opponent, and result information
- Supports multiple seasons and competitions

#### Match Statistics Collector (`match_stats_collector.py`)
- Collects detailed match statistics (possession, shots, passes, etc.)
- Implements progressive scraping with checkpoint recovery
- Handles large-scale data collection (1000+ matches)

#### Wages Collector (`wages_collector.py`)
- Scrapes team wage information from FBRef
- Processes player-level wage data
- Aggregates team-level financial metrics

#### Orchestration (`run_all_collectors.py`)
- Coordinates all data collection scripts
- Provides pipeline execution with dependency management
- Implements comprehensive logging and error reporting

### Data Engineering Pipeline

#### Master Dataset Creation (`master_creation_v1.ipynb`)
- Combines match statistics, fixtures, and wage data
- Creates rolling 5-match form averages
- Generates opponent perspective features
- Implements feature engineering for ML models

Key features created:
- **Form Metrics**: Rolling averages for all statistics over last 5 matches
- **Opponent Features**: Historical performance of upcoming opponents
- **Rest Days**: Days between matches for fatigue modeling
- **Wage Differentials**: Financial advantage metrics
- **Contextual Features**: Venue, competition, day of week effects

### Machine Learning Models

#### LightGBM Gradient Boosting Classifier (Model v2) - **PRIMARY MODEL**
- **Task**: Multi-class classification (Win/Draw/Loss) prediction
- **Algorithm**: LightGBM with Gradient Boosting Decision Trees (GBDT)
- **Features**: 192 carefully selected features including:
  - Financial metrics (wage differentials, team budgets)
  - Form metrics (5-match rolling averages for all statistics)
  - Opponent analysis (historical performance against upcoming opponents)
  - Contextual features (venue, competition, day of week)
- **Optimization**: Optuna hyperparameter tuning (100 trials, TPE sampler)
- **Validation**: 5-fold Stratified cross-validation
- **Target Encoding**: D=0, L=1, W=2 for multiclass prediction

**Model Performance (2024-2025 Premier League):**
- **Test Accuracy**: 49.21% (374 correct out of 760 matches)
- **Cross-Validation**: 55.86% average accuracy
- **Per-Class Performance**: Win (62.4% recall), Draw (4.8% recall), Loss (64.8% recall)
- **Challenge**: Draw prediction difficulty (common in football prediction)

#### XGBoost Random Forest Regressor (Model v3) - **EXPERIMENTAL**
- **Task**: Goal difference regression (-3 to +3 goals, capped)
- **Secondary Task**: Match outcome classification via threshold conversion
- **Features**: 197 engineered features including advanced composite metrics
- **Optimization**: Optuna hyperparameter tuning with 100 trials
- **Validation**: Time-series cross-validation for temporal consistency

**Model Performance:**
- **Regression**: MAE = 1.39 goals, R² = 0.068
- **Classification**: 47.4% accuracy on match outcomes
- **Prediction Accuracy**: 46.3% within ±1 goal difference

#### Feature Importance (Top 10 - LightGBM)
1. **avg_wage_dollars_diff**: Wage advantage over opponent (2,240 importance)
2. **max_wage_dollars_diff**: Top player wage differential (1,406 importance)
3. **total_wage_bill_dollars_diff**: Total squad wage advantage (1,085 importance)
4. **min_wage_dollars_diff**: Minimum wage differential (784 importance)
5. **venue**: Home/Away advantage (665 importance)
6. **age_mean_diff**: Squad age differential (594 importance)
7. **avg_wage_dollars**: Team average wage (575 importance)
8. **opp_avg_wage_dollars**: Opponent average wage (553 importance)
9. **Touches_favor_form_avg**: Team ball control metrics (534 importance)
10. **Tackles_favor_form_avg**: Team defensive activity (490 importance)

## Configuration Management

### Environment-Based Configuration
The project uses YAML-based configuration for different environments:

#### Production Configuration (`prod.yaml`)
- Complete seasonal data (2019-2025)
- All Premier League teams
- Conservative scraping delays (3-8 seconds)
- Full feature pipeline enabled

#### Development Configuration (`dev.yaml`) 
- Limited teams (Arsenal, Manchester City, Liverpool)
- Recent seasons only (2023-2025)
- Faster scraping delays (1-3 seconds)
- 100 match limit for testing

#### Testing Configuration (`testing.yaml`)
- Minimal dataset (Arsenal, Liverpool only)
- Current season only
- Ultra-fast delays (0.5-1.5 seconds)
- 10 match limit for validation

### Usage Examples
```bash
# Production data collection
python run_all_collectors_config.py --config prod

# Development testing
python run_all_collectors_config.py --config dev

# Quick validation
python run_all_collectors_config.py --config testing
```

## Data Schema

### Match Statistics Master Dataset

#### Core Match Information
- `date`: Match date (YYYY-MM-DD)
- `team_name`: Team name
- `opponent`: Opponent team name  
- `venue`: Home/Away/Neutral
- `result`: W/D/L from team perspective
- `season`: Season identifier (YYYY-YYYY)
- `comp`: Competition name

#### Statistical Features (64+ metrics)
- **Possession**: Ball possession percentage
- **Shots**: Shots on target, total shots
- **Passing**: Pass accuracy, total passes
- **Defensive**: Tackles, interceptions, clearances
- **Set Pieces**: Corners, throw-ins, free kicks
- **Physical**: Aerial duels, fouls committed/received

#### Engineered Features
- **Form Metrics**: 5-match rolling averages for all statistics
- **Opponent Analysis**: Historical performance against upcoming opponents
- **Rest Days**: Recovery time between matches
- **Wage Metrics**: Team and opponent financial data
- **Contextual**: Day of week, competition type effects

#### Target Variables
- `goal_diff`: Goal difference (-9 to +9, capped)
- `result`: Match outcome classification
- `goals_for`/`goals_against`: Actual goals scored/conceded

## Technical Implementation Details

### Web Scraping Architecture

#### Scraping Strategy
- **Requests + BeautifulSoup**: HTTP requests with HTML parsing (no browser automation)
- **Enhanced Anti-Detection**: User agent rotation, session management, and retry strategies
- **Progressive Loading**: Batch processing with checkpoint recovery
- **Rate Limiting**: Configurable delays (15-30 seconds between requests)
- **Error Recovery**: Automatic retry with exponential backoff

#### Data Extraction Pipeline
1. **HTTP Requests**: Direct requests to FBRef pages with proper headers
2. **Table Parsing**: Beautiful Soup HTML table extraction
3. **Data Validation**: Real-time data quality checks
4. **Format Conversion**: Multi-format output (JSON/CSV/Parquet)

### Feature Engineering Pipeline

#### Rolling Statistics Implementation
```python
# 5-match form calculation
def calculate_form_metrics(df, metrics, window=5):
    for metric in metrics:
        df[f'{metric}_form_avg'] = df[metric].rolling(window=window, min_periods=1).mean()
        df[f'{metric}_form_sum'] = df[metric].rolling(window=window, min_periods=1).sum()
```

#### Opponent Feature Generation
- Historical performance lookup for upcoming opponents
- Cross-referencing team statistics across seasons
- Temporal alignment for fair comparison

### Model Training Pipeline

#### Data Preparation
1. **Missing Value Handling**: Median imputation for numerical features (63,822 → 0 missing values)
2. **Feature Selection**: 192 features selected from 258 original features for LightGBM
3. **Temporal Splitting**: Training on 2019-2024 (4,758 matches), testing on 2024-2025 (760 matches)
4. **Target Encoding**: Label encoding for multiclass prediction (D=0, L=1, W=2)
5. **Class Distribution**: Training set balanced (D: 1,039, L: 1,715, W: 2,004)

#### Hyperparameter Optimization
- **Framework**: Optuna TPE Sampler
- **Objective**: Minimize Mean Absolute Error (MAE)
- **Validation**: 5-fold Time Series Cross-Validation
- **Search Space**: 12 hyperparameters optimized over 100 trials

#### Model Evaluation Framework
- **Regression Metrics**: MAE, MSE, RMSE, R²
- **Classification Metrics**: Accuracy, Precision, Recall, F1-Score
- **Custom Metrics**: Prediction accuracy within goal thresholds

## Dependencies and Requirements

### Core Dependencies
```
# Data Collection
requests>=2.31.0
beautifulsoup4>=4.12.0
urllib3>=2.0.0

# Data Processing
pandas>=2.1.0
numpy>=1.24.0
pyarrow>=12.0.0
orjson>=3.9.0

# Machine Learning
scikit-learn>=1.3.0
lightgbm>=4.0.0
xgboost>=2.0.0
optuna>=3.0.0
joblib>=1.3.0

# Development
pytest>=7.4.0
pytest-cov>=4.1.0
structlog>=23.1.0
```

### System Requirements
- **Python**: 3.8+
- **Memory**: 8GB+ RAM recommended for full dataset processing
- **Storage**: 5GB+ for complete historical data
- **Network**: Stable internet connection for web scraping

## Performance Metrics

### Data Collection Performance
- **Speed**: ~500-1000 matches per hour (with rate limiting)
- **Reliability**: 95%+ success rate with retry logic
- **Coverage**: 27 teams, 6 seasons, 5700+ matches collected
- **Storage Efficiency**: Parquet format reduces storage by 60%

### Model Performance Summary
- **LightGBM Classification**: 49.2% test accuracy (55.9% CV) - **PRODUCTION MODEL**
- **XGBoost Regression**: MAE = 1.39 goals (R² = 0.068) - **EXPERIMENTAL**
- **Training Time**: ~15-30 minutes for hyperparameter optimization
- **Inference Speed**: <1ms per prediction
- **Benchmark Comparison**: Significantly outperforms random prediction (33.3%)

### Resource Usage
- **Memory**: 15.7MB for master dataset (5,711 matches, 258 features)
- **Model Size**: <50MB for trained XGBoost model
- **Processing Speed**: ~1000 predictions per second

## Deployment and Usage

### Production Workflow
1. **Data Collection**: Run configured data collection pipeline
2. **Data Engineering**: Process raw data into master dataset
3. **Model Training**: Train and optimize ML models
4. **Prediction**: Generate predictions for upcoming matches
5. **Evaluation**: Monitor model performance and retrain as needed

### Command Line Interface
```bash
# Complete pipeline execution
python run_all_collectors_config.py --config prod

# Individual components
python team_id_mapper_config.py --config dev
python fixtures_collector_config.py --config dev
python match_stats_collector_config.py --config dev --max-matches 100

# Model training (via Jupyter notebooks)
jupyter lab notebooks/exploratory/machine_learning/model_v3.ipynb
```

### Configuration Customization
Users can create custom YAML configurations for specific use cases:
- Team-specific analysis
- Competition-specific models
- Custom feature sets
- Alternative scraping strategies

## Future Development Areas

### Immediate Enhancements
1. **Real-time Data**: Live match data integration
2. **Additional Leagues**: La Liga, Serie A expansion
3. **Advanced Features**: Player-level statistics, injury data
4. **Model Improvements**: Ensemble methods, deep learning approaches

### Long-term Roadmap
1. **Web Dashboard**: Interactive prediction interface
2. **API Development**: RESTful API for predictions
3. **Mobile App**: Mobile-friendly prediction platform
4. **Advanced Analytics**: Expected goals (xG) modeling, tactical analysis

### Technical Improvements
1. **Distributed Processing**: Spark/Dask for large-scale processing
2. **Cloud Deployment**: AWS/Azure deployment pipeline
3. **Model Monitoring**: MLOps pipeline with drift detection
4. **Automated Retraining**: Scheduled model updates

## Troubleshooting Guide

### Common Issues and Solutions

#### Data Collection Issues
- **Rate Limiting**: Increase delays in configuration
- **Website Changes**: Update CSS selectors in scraping scripts
- **Memory Issues**: Process data in smaller batches

#### Model Training Issues
- **Poor Performance**: Review feature engineering and data quality
- **Overfitting**: Adjust regularization parameters
- **Slow Training**: Reduce number of optimization trials

#### Configuration Issues
- **File Not Found**: Ensure YAML files exist in config/ directory
- **Invalid YAML**: Validate YAML syntax and structure
- **Permission Errors**: Check file system permissions

### Debugging Tools
- **Logging**: Comprehensive logging throughout pipeline
- **Progress Tracking**: Real-time progress indicators
- **Data Validation**: Built-in data quality checks
- **Error Recovery**: Automatic checkpoint and resume functionality

## Contact and Support

This technical documentation provides a comprehensive overview of the Football Predictions Project architecture, implementation, and usage. For specific questions or issues, refer to the project notebooks and scripts for detailed implementation examples.

---

*Last Updated: 2025-08-12*  
*Project Version: 3.0*  
*Documentation Version: 1.0*