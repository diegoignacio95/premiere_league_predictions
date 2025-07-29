#!/usr/bin/env python3
"""
Team ID Mapping Script (Configuration-based)

This script extracts team IDs and names from Premier League seasons on FBRef
and creates a comprehensive mapping of all teams that have played in the league.

Uses YAML configuration files instead of command-line arguments.

Based on: notebooks/exploratory/team_id_mapping.ipynb
"""

import sys
import os
from typing import Dict, List, Set
import argparse
import logging

# Add the scripts directory to the path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.scraping_utils import get_page, create_fbref_url
from utils.data_utils import save_json_data
from utils.text_utils import extract_team_id_from_href, validate_team_id_format
from utils.config_utils import load_config, setup_logging_from_config


def extract_team_ids_from_season(season: str) -> Dict[str, Dict]:
    """
    Extract team IDs and names from a specific Premier League season.
    
    Args:
        season: Season in format "2023-2024"
        
    Returns:
        Dictionary mapping team names to team data
    """
    logging.info(f"Fetching teams for {season}...")
    
    url = create_fbref_url("comps/9", season=season)
    soup = get_page(url)
    
    if not soup:
        logging.error(f"Failed to fetch page for {season}")
        return {}
    
    # Find the squads table
    table = soup.find('table', {'id': 'stats_squads_standard_for'})
    
    if not table:
        logging.warning(f"Squads table not found for {season}")
        return {}
    
    logging.info(f"Found table: {table.get('id', 'Unknown ID')}")
    
    team_mapping = {}
    tbody = table.find('tbody')
    
    if tbody:
        rows = tbody.find_all('tr')
        logging.info(f"Found tbody with {len(rows)} rows")
    else:
        rows = table.find_all('tr')
        logging.info(f"No tbody found, found {len(rows)} rows directly in table")
        # Skip header row if no tbody
        rows = [row for row in rows if row.find('td')]
        logging.info(f"After filtering header rows: {len(rows)} data rows")
    
    logging.info(f"Processing {len(rows)} rows...")
    
    for row in rows:
        # Look for team cell (th element with data-stat='team')
        team_cell = row.find('th', {'data-stat': 'team'})
        if not team_cell:
            continue
            
        # Extract team link
        team_link = team_cell.find('a')
        if not team_link:
            continue
            
        team_name = team_link.text.strip()
        team_href = team_link.get('href')
        
        # Extract team ID from href
        if team_href and '/squads/' in team_href:
            team_id = extract_team_id_from_href(team_href)
            if team_id and validate_team_id_format(team_id):
                team_mapping[team_name] = {
                    'team_id': team_id,
                    'season': season,
                    'href': team_href
                }
                logging.info(f"  + {team_name}: {team_id}")
            else:
                logging.warning(f"  x Invalid team ID for {team_name}: {team_id}")
    
    logging.info(f"Completed {season}: {len(team_mapping)} teams")
    return team_mapping


def extract_all_team_ids(seasons: List[str]) -> Dict[str, Dict]:
    """
    Extract team IDs for all specified seasons.
    
    Args:
        seasons: List of seasons to process
        
    Returns:
        Dictionary mapping team_id to team data with all seasons
    """
    logging.info(f"Starting team ID extraction for {len(seasons)} seasons")
    
    all_teams = {}
    
    for season in seasons:
        season_teams = extract_team_ids_from_season(season)
        
        for team_name, team_data in season_teams.items():
            team_id = team_data['team_id']
            
            if team_id not in all_teams:
                all_teams[team_id] = {
                    'team_name': team_name,
                    'team_id': team_id,
                    'seasons': [],
                    'aliases': set([team_name])  # Track different name variations
                }
            
            # Add season and track name variations
            all_teams[team_id]['seasons'].append(season)
            all_teams[team_id]['aliases'].add(team_name)
    
    # Convert sets to lists for JSON serialization
    for team_data in all_teams.values():
        team_data['aliases'] = list(team_data['aliases'])
    
    # Summary statistics
    total_teams = len(all_teams)
    total_seasons = sum(len(team_data['seasons']) for team_data in all_teams.values())
    
    logging.info(f"Extraction complete:")
    logging.info(f"  Total unique teams: {total_teams}")
    logging.info(f"  Total team-season combinations: {total_seasons}")
    
    return all_teams


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Extract Premier League team IDs from FBRef using configuration')
    parser.add_argument(
        '--config', 
        default='prod',
        help='Configuration to use (prod, dev, testing, or path to config file)'
    )
    parser.add_argument(
        '--output-file',
        help='Override output file path from configuration'
    )
    parser.add_argument(
        '--seasons',
        nargs='+',
        help='Override seasons from configuration'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually doing it'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Setup logging from configuration
        setup_logging_from_config(config)
        
        # Print configuration summary
        config.print_summary()
        
        # Override configuration if command line arguments provided
        seasons = args.seasons if args.seasons else config.get_effective_seasons()
        
        # Check if step is enabled
        if not config.is_step_enabled('team_mapping'):
            logging.info("Team mapping step is disabled in configuration")
            print("Team mapping step is disabled in configuration - skipping")
            return 0
        
        # Determine output file
        if args.output_file:
            output_file = args.output_file
        else:
            output_file = config.get_raw_data_path('all_teams.json')
        
        # Check if should skip if exists
        if config.should_skip_if_exists('team_mapping') and os.path.exists(output_file):
            logging.info(f"Output file exists and skip_if_exists=true: {output_file}")
            print(f"Output file exists, skipping: {output_file}")
            return 0
        
        # Ensure output directory exists
        config.ensure_data_directories()
        
        logging.info("Starting Premier League team ID mapping extraction")
        logging.info(f"Configuration: {args.config}")
        logging.info(f"Seasons to process: {seasons}")
        logging.info(f"Output file: {output_file}")
        
        if args.dry_run:
            print("DRY RUN - Would extract team IDs for:")
            print(f"  Seasons: {seasons}")
            print(f"  Output: {output_file}")
            return 0
        
        # Extract team IDs
        all_teams = extract_all_team_ids(seasons)
        
        if all_teams:
            # Save results
            save_json_data(all_teams, os.path.splitext(output_file)[0])
            logging.info(f"Team mapping saved to {output_file}")
            
            # Print summary
            print(f"\nSuccessfully extracted {len(all_teams)} teams")
            print(f"Seasons processed: {len(seasons)}")
            print(f"Environment: {config.environment}")
            print(f"Output saved to: {output_file}")
        else:
            logging.error("No teams were extracted")
            return 1
            
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())