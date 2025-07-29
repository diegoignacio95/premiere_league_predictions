#!/usr/bin/env python3
"""
Configuration Utilities

This module provides utilities for loading and validating YAML configuration files
for the data collection pipeline.
"""

import os
import sys
import yaml
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class DataCollectionConfig:
    """
    Configuration class for data collection pipeline.
    
    Loads and validates YAML configuration files and provides convenient
    access to configuration parameters.
    """
    
    def __init__(self, config_file: str = None):
        """
        Initialize configuration from file.
        
        Args:
            config_file: Path to YAML configuration file. If None, looks for
                        default configurations in config/ directory.
        """
        self.config_file = config_file
        self.config_data = {}
        self._load_config()
        self._validate_config()
    
    def _find_config_file(self, config_name: str) -> str:
        """
        Find configuration file in the config directory.
        
        Args:
            config_name: Name of config (e.g., 'prod', 'dev', 'testing')
            
        Returns:
            Path to configuration file
            
        Raises:
            ConfigurationError: If config file not found
        """
        # Get the project root directory (3 levels up from scripts/utils/)
        current_dir = Path(__file__).parent
        project_root = current_dir.parent.parent
        config_dir = project_root / "config"
        
        # Try different filename patterns
        possible_files = [
            config_dir / f"{config_name}.yaml",
            config_dir / f"{config_name}.yml",
            config_dir / f"config_{config_name}.yaml",
            config_dir / f"config_{config_name}.yml"
        ]
        
        for config_path in possible_files:
            if config_path.exists():
                return str(config_path)
        
        # List available configs for error message
        available_configs = []
        if config_dir.exists():
            for file in config_dir.glob("*.yaml"):
                available_configs.append(file.stem)
            for file in config_dir.glob("*.yml"):
                if file.stem not in available_configs:
                    available_configs.append(file.stem)
        
        raise ConfigurationError(
            f"Configuration file not found for '{config_name}'. "
            f"Available configurations: {available_configs}"
        )
    
    def _load_config(self):
        """Load configuration from YAML file."""
        if self.config_file is None:
            # Default to prod configuration
            self.config_file = self._find_config_file("prod")
        elif not os.path.isabs(self.config_file):
            # If not absolute path, try to find it in config directory
            if not self.config_file.endswith(('.yaml', '.yml')):
                self.config_file = self._find_config_file(self.config_file)
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self.config_data = yaml.safe_load(f)
            
            logging.info(f"Loaded configuration from: {self.config_file}")
            
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found: {self.config_file}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")
    
    def _validate_config(self):
        """Validate configuration structure and required fields."""
        required_sections = ['data_collection']
        
        for section in required_sections:
            if section not in self.config_data:
                raise ConfigurationError(f"Missing required section: {section}")
        
        # Validate data_collection section
        dc_config = self.config_data['data_collection']
        required_dc_fields = ['environment', 'seasons', 'output']
        
        for field in required_dc_fields:
            if field not in dc_config:
                raise ConfigurationError(f"Missing required field in data_collection: {field}")
        
        # Validate environment
        valid_environments = ['dev', 'prod', 'test']
        if dc_config['environment'] not in valid_environments:
            raise ConfigurationError(
                f"Invalid environment: {dc_config['environment']}. "
                f"Must be one of: {valid_environments}"
            )
        
        # Validate seasons format
        if not isinstance(dc_config['seasons'], list) or not dc_config['seasons']:
            raise ConfigurationError("seasons must be a non-empty list")
        
        # Validate output formats
        output_config = dc_config.get('output', {})
        valid_formats = ['json', 'csv', 'parquet']
        formats = output_config.get('formats', [])
        
        if not isinstance(formats, list):
            raise ConfigurationError("output.formats must be a list")
        
        for fmt in formats:
            if fmt not in valid_formats:
                raise ConfigurationError(
                    f"Invalid output format: {fmt}. "
                    f"Must be one of: {valid_formats}"
                )
        
        logging.info("Configuration validation passed")
    
    @property
    def environment(self) -> str:
        """Get the environment setting."""
        return self.config_data['data_collection']['environment']
    
    @property
    def seasons(self) -> List[str]:
        """Get the list of seasons to process."""
        return self.config_data['data_collection']['seasons']
    
    @property
    def output_formats(self) -> List[str]:
        """Get the list of output formats."""
        return self.config_data['data_collection']['output']['formats']
    
    @property
    def base_path(self) -> str:
        """Get the base path for data storage."""
        return self.config_data['data_collection']['output']['base_path']
    
    @property
    def teams_filter(self) -> Optional[List[str]]:
        """Get the teams filter, if any."""
        return self.config_data['data_collection']['filters'].get('teams')
    
    @property
    def competitions_filter(self) -> List[str]:
        """Get the competitions filter."""
        return self.config_data['data_collection']['filters'].get('competitions', ['Premier League'])
    
    @property
    def seasons_filter(self) -> Optional[List[str]]:
        """Get the seasons filter override, if any."""
        return self.config_data['data_collection']['filters'].get('seasons')
    
    @property
    def max_matches(self) -> Optional[int]:
        """Get the maximum matches limit, if any."""
        return self.config_data['data_collection']['filters'].get('max_matches')
    
    @property
    def enhanced_scraper(self) -> bool:
        """Whether to use enhanced scraper."""
        return self.config_data['data_collection']['scraping'].get('enhanced_scraper', True)
    
    @property
    def progress_save(self) -> bool:
        """Whether to save progress periodically."""
        return self.config_data['data_collection']['scraping'].get('progress_save', True)
    
    @property
    def log_level(self) -> str:
        """Get the logging level."""
        return self.config_data['data_collection']['scraping'].get('log_level', 'INFO')
    
    @property
    def scraping_delays(self) -> Dict[str, float]:
        """Get scraping delay configuration."""
        return self.config_data['data_collection']['scraping'].get('delays', {
            'min': 3.0,
            'max': 12.0,
            'chunk_size': 50,
            'chunk_break': 300.0
        })
    
    @property
    def steps_config(self) -> Dict[str, Dict]:
        """Get the steps configuration."""
        return self.config_data['data_collection'].get('steps', {})
    
    def is_step_enabled(self, step_name: str) -> bool:
        """Check if a collection step is enabled."""
        step_config = self.steps_config.get(step_name, {})
        return step_config.get('enabled', True)
    
    def should_skip_if_exists(self, step_name: str) -> bool:
        """Check if a step should be skipped if output already exists."""
        step_config = self.steps_config.get(step_name, {})
        return step_config.get('skip_if_exists', False)
    
    def get_data_path(self, *path_parts: str) -> str:
        """
        Get a data path relative to the configured base path and environment.
        
        Args:
            *path_parts: Path components to join
            
        Returns:
            Complete path string
        """
        full_path = os.path.join(self.base_path, self.environment, *path_parts)
        return full_path
    
    def get_raw_data_path(self, *path_parts: str) -> str:
        """
        Get a raw data path.
        
        Args:
            *path_parts: Path components to join after 'raw/'
            
        Returns:
            Complete path to raw data file/directory
        """
        return self.get_data_path('raw', *path_parts)
    
    def ensure_data_directories(self):
        """Create necessary data directories if they don't exist."""
        directories = [
            self.get_data_path('raw'),
            self.get_data_path('raw', 'match_stats'),
            self.get_data_path('processed'),
            self.get_data_path('external')
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logging.debug(f"Ensured directory exists: {directory}")
    
    def get_effective_seasons(self) -> List[str]:
        """
        Get the effective list of seasons to process.
        
        Returns seasons filter if specified, otherwise all configured seasons.
        """
        return self.seasons_filter or self.seasons
    
    def get_effective_teams(self) -> Optional[List[str]]:
        """
        Get the effective list of teams to process.
        
        Returns teams filter if specified, otherwise None (all teams).
        """
        return self.teams_filter
    
    def to_dict(self) -> Dict[str, Any]:
        """Return the full configuration as a dictionary."""
        return self.config_data.copy()
    
    def print_summary(self):
        """Print a summary of the configuration."""
        print("Configuration Summary:")
        print(f"   Config file: {self.config_file}")
        print(f"   Environment: {self.environment}")
        print(f"   Seasons: {self.get_effective_seasons()}")
        print(f"   Output formats: {self.output_formats}")
        print(f"   Teams filter: {self.get_effective_teams() or 'All teams'}")
        print(f"   Competitions: {self.competitions_filter}")
        print(f"   Max matches: {self.max_matches or 'No limit'}")
        print(f"   Enhanced scraper: {self.enhanced_scraper}")
        print(f"   Log level: {self.log_level}")
        
        # Show enabled steps
        enabled_steps = [step for step in self.steps_config.keys() 
                        if self.is_step_enabled(step)]
        print(f"   Enabled steps: {enabled_steps}")


def load_config(config_name: str = None) -> DataCollectionConfig:
    """
    Convenience function to load a configuration.
    
    Args:
        config_name: Name of configuration to load ('prod', 'dev', 'testing')
                    or path to config file. If None, loads 'prod'.
                    
    Returns:
        DataCollectionConfig instance
    """
    if config_name is None:
        config_name = 'prod'
    
    return DataCollectionConfig(config_name)


def setup_logging_from_config(config: DataCollectionConfig):
    """
    Setup logging based on configuration.
    
    Args:
        config: DataCollectionConfig instance
    """
    log_config = config.config_data.get('logging', {})
    
    log_level = log_config.get('level', config.log_level)
    log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    log_file = log_config.get('file', 'data_collection.log')
    
    # Clear any existing handlers
    logging.getLogger().handlers.clear()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )
    
    logging.info(f"Logging configured: level={log_level}, file={log_file}")


if __name__ == "__main__":
    # Demo usage
    print("Configuration Utility Demo")
    print("=" * 40)
    
    try:
        # Load different configurations
        for config_name in ['prod', 'dev', 'testing']:
            print(f"\nLoading {config_name} configuration:")
            config = load_config(config_name)
            config.print_summary()
            
    except ConfigurationError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")