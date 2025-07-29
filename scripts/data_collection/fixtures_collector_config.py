#!/usr/bin/env python3
"""
Fixtures Collection Script (Configuration-based)

This script extracts match fixtures and results for Premier League teams
from FBRef across multiple seasons.

Uses YAML configuration files instead of command-line arguments.

Based on: notebooks/exploratory/scores_fixtures.ipynb
"""

import sys
import os
from typing import Dict, List, Optional
import argparse
import logging
import time

# Add the scripts directory to the path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.scraping_utils import get_page, create_fbref_url
from utils.data_utils import (
    load_teams_from_json, 
    save_json_data, 
    fixtures_data_to_dataframe,
    save_dataframe_to_multiple_formats
)
from utils.text_utils import clean_team_name_for_url
from utils.config_utils import load_config, setup_logging_from_config


def extract_scores_fixtures(team_id: str, season: str, team_name: str) -> Dict:
    """
    Extract scores and fixtures information for a team in a specific season.
    
    Args:
        team_id: FBRef team ID (e.g., 'b8fd03ef')
        season: Season in format '2023-2024'
        team_name: Team name for URL construction
    
    Returns:
        Dictionary containing match data
    """
    logging.info(f"Fetching fixtures for {team_name} ({season})...")
    
    url = create_fbref_url("squads", team_id=team_id, season=season, 
                          team_name=team_name, page_type="fixtures")
    
    logging.debug(f"URL: {url}")
    
    soup = get_page(url)
    if not soup:
        logging.error(f"Failed to fetch page for {team_name} ({season})")
        return {}
    
    # Look for fixtures table - uses 'matchlogs_for' table ID
    fixtures_table = soup.find('table', {'id': 'matchlogs_for'})
    
    if not fixtures_table:
        logging.warning(f"No fixtures table found for {team_name} ({season})")
        return {}
    
    logging.info(f"Found fixtures table for {team_name}")
    
    # Initialize fixtures data structure
    fixtures_data = {
        'team_id': team_id,
        'team_name': team_name,
        'season': season,
        'matches': []
    }
    
    # Process fixtures table
    tbody = fixtures_table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')
    else:
        rows = fixtures_table.find_all('tr')
        # Filter out header rows
        rows = [row for row in rows if row.find('td')]
    
    logging.info(f"Found {len(rows)} fixture rows")
    
    for row in rows:
        match_data = {}
        
        # Extract all available data columns
        cells = row.find_all(['td', 'th'])
        for cell in cells:
            data_stat = cell.get('data-stat')
            if data_stat:
                cell_text = cell.text.strip()
                if cell_text and cell_text != '':
                    match_data[data_stat] = cell_text
                    
                # Special handling for links (opponent, competition, etc.)
                cell_link = cell.find('a')
                if cell_link and data_stat:
                    href = cell_link.get('href')
                    if href:
                        match_data[f"{data_stat}_href"] = href
        
        # Only add match if we have meaningful data
        if match_data.get('date') or match_data.get('opponent'):
            fixtures_data['matches'].append(match_data)
    
    logging.info(f"Extracted {len(fixtures_data['matches'])} matches for {team_name}")
    return fixtures_data


def extract_all_team_fixtures(all_teams_dict: Dict[str, Dict], 
                            config, progress_save: bool = True) -> Dict:
    """
    Extract fixtures data for all teams across all seasons they played.
    
    Args:
        all_teams_dict: Dictionary containing team information with seasons
        config: Configuration object
        progress_save: Whether to save progress periodically
        
    Returns:
        Complete fixtures dataset organized by team_id and season
    """
    all_fixtures_data = {}
    total_extractions = sum(len(team_info['seasons']) for team_info in all_teams_dict.values())
    current_extraction = 0
    
    # Get output directory for progress saves
    output_dir = os.path.dirname(config.get_raw_data_path('all_competitions_fixtures.json'))
    
    logging.info(f"Starting fixtures extraction for {len(all_teams_dict)} teams "
                f"across {total_extractions} team-season combinations...")
    
    for team_id, team_info in all_teams_dict.items():
        team_name = team_info['team_name']
        seasons = team_info['seasons']
        
        logging.info(f"Processing {team_name} (ID: {team_id})")
        logging.info(f"   Seasons to extract: {seasons}")
        
        # Initialize team entry in results
        if team_id not in all_fixtures_data:
            all_fixtures_data[team_id] = {
                'team_name': team_name,
                'team_id': team_id,
                'seasons_data': {}
            }
        
        # Extract fixtures for each season this team played
        for season in seasons:
            current_extraction += 1
            logging.info(f"[{current_extraction}/{total_extractions}] "
                        f"Extracting {team_name} {season}...")
            
            try:
                season_fixtures = extract_scores_fixtures(team_id, season, team_name)
                
                if season_fixtures and season_fixtures.get('matches'):
                    all_fixtures_data[team_id]['seasons_data'][season] = season_fixtures
                    match_count = len(season_fixtures['matches'])
                    logging.info(f"   + Success: {match_count} matches")
                else:
                    logging.warning(f"   ! No fixture data found for {team_name} in {season}")
                    all_fixtures_data[team_id]['seasons_data'][season] = None
                    
            except Exception as e:
                logging.error(f"   x Error extracting {team_name} {season}: {str(e)}")
                all_fixtures_data[team_id]['seasons_data'][season] = None
            
            # Small delay to be respectful to the server
            time.sleep(1)
            
            # Save progress every 10 extractions
            if progress_save and current_extraction % 10 == 0:
                progress_file = f"fixtures_progress_{current_extraction}.json"
                save_json_data(all_fixtures_data, 
                             os.path.join(output_dir, progress_file.replace('.json', '')))
                logging.info(f"Progress saved at {current_extraction} extractions")
    
    # Summary statistics
    successful_extractions = 0
    total_matches = 0
    
    for team_data in all_fixtures_data.values():
        for season_data in team_data['seasons_data'].values():
            if season_data and season_data.get('matches'):
                successful_extractions += 1
                total_matches += len(season_data['matches'])
    
    logging.info(f"Extraction Summary:")
    logging.info(f"  Total teams processed: {len(all_fixtures_data)}")
    logging.info(f"  Successful extractions: {successful_extractions}/{total_extractions}")
    logging.info(f"  Total match records: {total_matches}")
    
    return all_fixtures_data


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Extract Premier League fixtures from FBRef using configuration')
    parser.add_argument(
        '--config', 
        default='prod',
        help='Configuration to use (prod, dev, testing, or path to config file)'
    )
    parser.add_argument(
        '--teams-file',
        help='Override teams file path from configuration'
    )
    parser.add_argument(
        '--output-file',
        help='Override output file path from configuration'
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
        
        # Check if step is enabled
        if not config.is_step_enabled('fixtures'):
            logging.info("Fixtures collection step is disabled in configuration")
            print("Fixtures collection step is disabled in configuration - skipping")
            return 0
        
        # Determine file paths
        teams_file = args.teams_file if args.teams_file else config.get_raw_data_path('all_teams.json')
        output_file = args.output_file if args.output_file else config.get_raw_data_path('all_competitions_fixtures.json')
        
        # Check if should skip if exists
        if config.should_skip_if_exists('fixtures') and os.path.exists(output_file):
            logging.info(f"Output file exists and skip_if_exists=true: {output_file}")
            print(f"Output file exists, skipping: {output_file}")
            return 0
        
        # Ensure output directory exists
        config.ensure_data_directories()
        
        logging.info("Starting Premier League fixtures collection")
        logging.info(f"Configuration: {args.config}")
        logging.info(f"Teams file: {teams_file}")
        logging.info(f"Output file prefix: {output_file}")
        logging.info(f"Output formats: {config.output_formats}")
        
        if args.dry_run:
            print("DRY RUN - Would collect fixtures using:")
            print(f"  Teams file: {teams_file}")
            print(f"  Output: {output_file}")
            print(f"  Formats: {config.output_formats}")
            return 0
        
        # Load teams data
        if not os.path.exists(teams_file):
            logging.error(f"Teams file not found: {teams_file}")
            print(f"Teams file not found: {teams_file}")
            print("Please run team_id_mapper first to generate the teams file.")
            return 1
        
        all_teams = load_teams_from_json(teams_file)
        logging.info(f"Loaded {len(all_teams)} teams from {teams_file}")
        
        # Apply team filter if specified
        if config.get_effective_teams():
            filtered_teams = {}
            for team_id, team_data in all_teams.items():
                if team_data['team_name'] in config.get_effective_teams():
                    filtered_teams[team_id] = team_data
            all_teams = filtered_teams
            logging.info(f"Filtered to {len(all_teams)} teams: {config.get_effective_teams()}")
        
        # Apply season filter if specified
        effective_seasons = config.get_effective_seasons()
        if effective_seasons != config.seasons:
            for team_data in all_teams.values():
                team_data['seasons'] = [s for s in team_data['seasons'] if s in effective_seasons]
            logging.info(f"Filtered to seasons: {effective_seasons}")
        
        if not all_teams:
            logging.error("No teams to process after filtering")
            return 1
        
        # Extract fixtures
        fixtures_data = extract_all_team_fixtures(
            all_teams, 
            config,
            progress_save=config.progress_save
        )
        
        if fixtures_data:
            # Save raw fixtures data
            save_json_data(fixtures_data, os.path.splitext(output_file)[0])
            
            # Convert to DataFrame and save in requested formats
            fixtures_df = fixtures_data_to_dataframe(fixtures_data)
            
            if not fixtures_df.empty:
                base_name = os.path.splitext(output_file)[0] + "_dataframe"
                save_dataframe_to_multiple_formats(
                    fixtures_df, 
                    base_name,
                    formats=config.output_formats
                )
                
                # Print summary
                print(f"\nSuccessfully extracted fixtures for {len(fixtures_data)} teams")
                print(f"Total matches: {len(fixtures_df)}")
                print(f"Environment: {config.environment}")
                print(f"Raw data saved to: {output_file}")
                for fmt in config.output_formats:
                    print(f"DataFrame saved to: {base_name}.{fmt}")
            else:
                logging.warning("No fixtures data was converted to DataFrame")
        else:
            logging.error("No fixtures were extracted")
            return 1
            
    except Exception as e:
        logging.error(f"Fixtures collection failed: {str(e)}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())