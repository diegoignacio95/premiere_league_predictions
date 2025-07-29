# Data Collection Strategy - Squad Information Table

## Overview
This document outlines the strategy for collecting Premier League squad information to populate our machine learning dataset.

## Target Schema
```sql
squad_id (INT, PRIMARY KEY)
squad_name (VARCHAR)
season (VARCHAR)
num_players_payroll (INT)
squad_income (DECIMAL)
min_age_squad (INT)
max_age_squad (INT)
avg_age_squad (DECIMAL)
stadium_capacity (INT)
ascending_squad (BOOLEAN)
```

## Data Sources

### Primary Sources
1. **FBRef.com** (Football Reference)
   - Squad rosters and player ages
   - Squad statistics per season
   - Reliable historical data
   - Already have scraper notebook

2. **Transfermarkt.com**
   - Squad market values (proxy for income)
   - Player ages and squad composition
   - Transfer data for promoted teams

3. **Premier League Official API/Website**
   - Official squad lists
   - Stadium information
   - Promotion/relegation data

### Secondary Sources
4. **Football-Data.co.uk**
   - Historical league tables
   - Promotion indicators

5. **Wikipedia**
   - Stadium capacity information
   - Team historical data

## Data Collection Plan

### Phase 1: Core Squad Data
- **Source**: FBRef.com
- **Fields**: squad_name, season, player ages (for aggregation)
- **Method**: Extend existing `fbref_scrapper.ipynb`
- **Output**: Raw player data per squad per season

### Phase 2: Financial Data
- **Source**: Transfermarkt.com
- **Fields**: squad_income (estimated from market values)
- **Method**: New scraper for squad valuations
- **Challenge**: Convert market value to income estimate

### Phase 3: Stadium & Promotion Data
- **Source**: Multiple (Wikipedia, Premier League site)
- **Fields**: stadium_capacity, ascending_squad
- **Method**: Static data collection + annual updates
- **Note**: Stadium capacity changes infrequently

## Implementation Strategy

### 1. Data Aggregation Requirements
```python
# From player-level data, calculate:
min_age_squad = min(player_ages)
max_age_squad = max(player_ages)
avg_age_squad = mean(player_ages)
num_players_payroll = count(players)
```

### 2. Squad Income Estimation
```python
# Proxy methods:
# Method 1: Sum of player market values / 10 (rough conversion)
# Method 2: Use reported revenue data where available
# Method 3: League position correlation model
```

### 3. Unique Squad ID Generation
```python
# Format: CLUB_CODE + SEASON_CODE
# Example: MUN2324 (Manchester United 2023-24)
squad_id = f"{club_code}{season_code}"
```

## Scraping Schedule

### Historical Data (2010-2024)
1. **Week 1-2**: FBRef squad rosters (all seasons)
2. **Week 3**: Transfermarkt valuations
3. **Week 4**: Stadium and promotion data
4. **Week 5**: Data validation and cleaning

### Ongoing Data (Current Season)
- **Monthly**: Update current season squad changes
- **Transfer Windows**: Update player movements
- **Season End**: Add promotion/relegation flags

## Technical Considerations

### Rate Limiting
- FBRef: 1 request per 3 seconds
- Transfermarkt: 1 request per 5 seconds
- Implement delays and retry logic

### Data Storage
```
data/raw/
├── fbref_squads/
├── transfermarkt_values/
├── stadium_data/
└── promotion_data/

data/processed/
└── squad_information.csv
```

### Error Handling
- Missing player age data
- Clubs changing names
- Promoted teams with limited history
- Website structure changes

## Validation Rules

1. **Squad Name Consistency**: Standardize team names across sources
2. **Season Format**: Ensure consistent YYYY-YY format
3. **Age Validation**: Players between 16-45 years
4. **Capacity Validation**: Stadium capacity > 10,000
5. **Income Validation**: Positive values, realistic ranges

## Expected Challenges

1. **Website Changes**: Scrapers may break with site updates
2. **Missing Data**: Some historical financial data unavailable
3. **Promoted Teams**: Limited historical data for newly promoted teams
4. **Data Consistency**: Different sources may have conflicting information

## Success Metrics

- **Coverage**: 95%+ of Premier League teams per season
- **Completeness**: <5% missing values per field
- **Accuracy**: Manual validation of 10% random sample
- **Timeliness**: Data updated within 1 week of source updates