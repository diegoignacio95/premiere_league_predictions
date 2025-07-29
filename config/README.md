# Configuration System

The data collection pipeline now uses YAML configuration files instead of command-line arguments. This provides better organization, documentation, and reusability of collection parameters.

## Available Configurations

### `prod.yaml` - Production Configuration
- **Environment**: `prod`
- **Seasons**: Complete range (2019-2020 to 2024-2025)
- **Teams**: All teams
- **Output**: JSON and CSV formats
- **Scraping**: Conservative delays, enhanced scraper
- **Steps**: All enabled

Use for complete, production-ready data collection.

### `dev.yaml` - Development Configuration
- **Environment**: `dev`
- **Seasons**: Recent seasons only (2023-2024, 2024-2025)
- **Teams**: Limited to Arsenal, Manchester City, Liverpool
- **Output**: JSON and CSV formats
- **Scraping**: Faster delays, enhanced scraper
- **Match Limit**: 100 matches for faster testing
- **Steps**: All enabled, but with skip_if_exists=true

Use for development and testing with faster execution.

### `testing.yaml` - Quick Testing Configuration
- **Environment**: `dev`
- **Seasons**: Current season only (2024-2025)
- **Teams**: Arsenal and Liverpool only
- **Output**: JSON only
- **Scraping**: Minimal delays, basic scraper
- **Match Limit**: 10 matches for very quick validation
- **Steps**: Team mapping, fixtures, and match stats only (wages disabled)

Use for quick validation and testing.

## Usage

### Basic Usage

```bash
# Use production configuration
python team_id_mapper_config.py --config prod

# Use development configuration
python fixtures_collector_config.py --config dev

# Use testing configuration for quick validation
python run_all_collectors_config.py --config testing
```

### Run Complete Pipeline

```bash
# Production data collection
python run_all_collectors_config.py --config prod

# Development with faster settings
python run_all_collectors_config.py --config dev

# Quick testing (just 10 matches)
python run_all_collectors_config.py --config testing

# Dry run to see what would be executed
python run_all_collectors_config.py --config prod --dry-run
```

### Individual Scripts

```bash
# Team ID mapping
python team_id_mapper_config.py --config prod

# Fixtures collection
python fixtures_collector_config.py --config dev

# Run specific step only
python run_all_collectors_config.py --config prod --step fixtures

# Skip existing files
python run_all_collectors_config.py --config prod --skip-existing
```

## Configuration Structure

Each YAML file contains the following sections:

### `data_collection`
- `environment`: Data environment (dev/prod)
- `seasons`: List of seasons to process
- `output`: Output formats and paths
- `filters`: Team, competition, and match filters
- `scraping`: Scraping behavior and delays
- `steps`: Enable/disable individual collection steps

### `logging`
- `level`: Logging verbosity
- `format`: Log message format
- `file`: Log file name

## Environment Management

The configuration system automatically handles environment-specific paths:

- **dev environment**: `data/dev/raw/`
- **prod environment**: `data/prod/raw/`

This allows you to:
- Test changes safely in the dev environment
- Keep production data separate and stable
- Switch between environments easily

## Creating Custom Configurations

1. Copy an existing configuration file
2. Modify the parameters as needed
3. Save with a descriptive name (e.g., `custom.yaml`)
4. Use with `--config custom`

### Example Custom Configuration

```yaml
data_collection:
  environment: "dev"
  seasons: ["2024-2025"]
  
  output:
    formats: ["json", "csv", "parquet"]
    base_path: "../../data"
  
  filters:
    teams: ["Arsenal", "Chelsea", "Tottenham"]  # London clubs only
    competitions: ["Premier League"]
    max_matches: 50
  
  scraping:
    enhanced_scraper: true
    log_level: "INFO"
    delays:
      min: 2.0
      max: 8.0
  
  steps:
    team_mapping:
      enabled: true
      skip_if_exists: true
    fixtures:
      enabled: true
      skip_if_exists: false
    wages:
      enabled: false  # Skip wages for faster execution
    match_stats:
      enabled: true
      skip_if_exists: false

logging:
  level: "INFO"
  file: "london_clubs_collection.log"
```

## Migration from Command-Line Scripts

The original command-line scripts are still available with `_config` suffix:

| Original Script | New Configuration-Based Script |
|----------------|--------------------------------|
| `team_id_mapper.py` | `team_id_mapper_config.py` |
| `fixtures_collector.py` | `fixtures_collector_config.py` |
| `wages_collector.py` | `wages_collector_config.py` |
| `match_stats_collector.py` | `match_stats_collector_config.py` |
| `run_all_collectors.py` | `run_all_collectors_config.py` |

## Benefits

✅ **Centralized Configuration**: All settings in one place  
✅ **Environment Management**: Easy dev/prod separation  
✅ **Documentation**: Comments explain each parameter  
✅ **Reusability**: Save and share collection scenarios  
✅ **Version Control**: Track parameter changes  
✅ **Validation**: Automatic parameter validation  
✅ **Less Verbose**: No long command-line arguments  

## Troubleshooting

### Configuration File Not Found
```
Configuration file not found for 'myconfig'
```
- Check that the file exists in the `config/` directory
- Verify the filename (should be `myconfig.yaml`)
- Use `--config path/to/config.yaml` for files outside config directory

### Invalid Configuration
```
Missing required section: data_collection
```
- Ensure your YAML file has the required structure
- Check YAML syntax (indentation, colons, etc.)
- Refer to existing configurations as templates

### Permission Errors
```
Error loading configuration: Permission denied
```
- Check file permissions
- Ensure the config directory is readable

### Environment Issues
```
Invalid environment: test. Must be one of: ['dev', 'prod', 'test']
```
- Use only supported environments: `dev`, `prod`, or `test`
- Check spelling in your configuration file