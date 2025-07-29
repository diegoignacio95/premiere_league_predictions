# FBRef Wage Scraping - Step by Step Strategy

## Current Status
✅ **Existing Infrastructure**
- Basic scraping functions in `fbref_scrapper.ipynb`
- Rate limiting and headers already implemented
- DataFrame handling established

## Step-by-Step Implementation Plan

### Step 1: Understand Team ID Mapping
**Challenge**: We need to map team names to FBRef team IDs
- Manchester City = `b8fd03ef`
- URL Pattern: `/en/squads/{team_id}/{season}/wages/{team-name}-Wage-Details`

**Action**: Create a team mapping function

### Step 2: Extract Team IDs from League Page
**Method**: Scrape the main Premier League page to get all team links
```python
# From: https://fbref.com/en/comps/9/2023-2024/2023-2024-Premier-League-Stats
# Extract all team links to get team_id mappings
```

### Step 3: Build Wage Scraping Function
**Target Data Structure**:
```python
{
    'team_id': 'b8fd03ef',
    'team_name': 'Manchester City',
    'season': '2022-2023',
    'total_wage_bill': 400000000,  # Sum of all wages
    'avg_wage': 16000000,         # Average player wage
    'num_players': 25,            # Count of players
    'top_earner_wage': 20800000   # Highest paid player
}
```

### Step 4: Handle Different Seasons
**URL Format**: 
- 2022-2023 season: `/en/squads/{team_id}/2022-2023/wages/`
- 2023-2024 season: `/en/squads/{team_id}/2023-2024/wages/`

### Step 5: Error Handling & Data Quality
**Common Issues**:
- Missing wage pages for some teams
- "Unverified estimation" flags
- Currency conversion consistency
- Player loan situations

### Step 6: Scale to All Premier League Teams
**Process**:
1. Get all team IDs from league table
2. Loop through each team for target seasons
3. Extract and aggregate wage data
4. Save to structured format

## Implementation Order

### Phase 1: Single Team Test (Manchester City)
- [ ] Create basic wage scraping function
- [ ] Test with Manchester City 2022-2023
- [ ] Validate data extraction

### Phase 2: Team ID Discovery
- [ ] Extract all Premier League team IDs
- [ ] Create team mapping dictionary
- [ ] Handle team name variations

### Phase 3: Multi-Team Implementation
- [ ] Loop through all teams
- [ ] Add error handling for missing pages
- [ ] Implement data validation

### Phase 4: Multi-Season Support
- [ ] Add season parameter
- [ ] Handle historical data (2019-2024)
- [ ] Deal with promoted/relegated teams

## Technical Considerations

### Rate Limiting
```python
time.sleep(random.uniform(2, 4))  # Increase delay for wage pages
```

### Data Storage
```
data/raw/wages/
├── 2022-2023/
│   ├── manchester_city_wages.csv
│   ├── arsenal_wages.csv
│   └── ...
└── 2023-2024/
    ├── manchester_city_wages.csv
    └── ...
```

### Squad Income Calculation
```python
def calculate_squad_income(wage_data):
    # Convert weekly wages to annual
    annual_wages = wage_data['weekly_wage'] * 52
    return {
        'total_wage_bill': annual_wages.sum(),
        'avg_wage': annual_wages.mean(),
        'median_wage': annual_wages.median(),
        'top_earner': annual_wages.max()
    }
```

## Next Actions
1. **Start with Step 1**: Create team ID mapping function
2. **Test with one team**: Manchester City wage scraping
3. **Iterate and improve**: Add error handling
4. **Scale gradually**: Add more teams one by one