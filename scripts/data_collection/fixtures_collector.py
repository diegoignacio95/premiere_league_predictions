#!/usr/bin/env python3
"""
Fixtures Collection Script

This script extracts match fixtures and results for Premier League teams
from FBRef across multiple seasons.

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


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('fixtures_collection.log')
        ]
    )


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
                            progress_save: bool = True,
                            output_dir: str = "../../data/raw/") -> Dict:
    """
    Extract fixtures data for all teams across all seasons they played.
    
    Args:
        all_teams_dict: Dictionary containing team information with seasons
        progress_save: Whether to save progress periodically
        output_dir: Directory for progress saves
        
    Returns:
        Complete fixtures dataset organized by team_id and season
    """
    all_fixtures_data = {}
    total_extractions = sum(len(team_info['seasons']) for team_info in all_teams_dict.values())
    current_extraction = 0
    
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
                    logging.info(f"   ✅ Success: {match_count} matches")
                else:
                    logging.warning(f"   ⚠️  No fixture data found for {team_name} in {season}")
                    all_fixtures_data[team_id]['seasons_data'][season] = None
                    
            except Exception as e:
                logging.error(f"   ❌ Error extracting {team_name} {season}: {str(e)}")
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
    parser = argparse.ArgumentParser(description='Extract Premier League fixtures from FBRef')
    parser.add_argument(
        '--environment',
        default='prod',
        choices=['dev', 'prod'],
        help='Data environment (default: prod)'
    )
    parser.add_argument(
        '--teams-file',
        help='Path to teams JSON file (optional, will use environment-based path if not specified)'
    )
    parser.add_argument(
        '--output-file',
        help='Output file prefix (optional, will use environment-based path if not specified)'
    )
    parser.add_argument(
        '--output-formats',
        nargs='+',
        default=['json'],
        choices=['json', 'csv', 'parquet'],
        help='Output formats (default: json)'
    )
    parser.add_argument(
        '--teams',
        nargs='+',
        help='Specific teams to process (by name, optional)'
    )
    parser.add_argument(
        '--seasons',
        nargs='+',
        help='Specific seasons to process (optional)'
    )
    parser.add_argument(
        '--progress-save',
        action='store_true',
        default=True,
        help='Save progress periodically (default: True)'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Construct file paths based on environment
    if args.teams_file:
        teams_file = args.teams_file
    else:
        teams_file = f'../../data/{args.environment}/raw/all_teams.json'
    
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = f'../../data/{args.environment}/raw/all_competitions_fixtures'
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info("Starting Premier League fixtures collection")
    logging.info(f"Environment: {args.environment}")
    logging.info(f"Teams file: {teams_file}")
    logging.info(f"Output file prefix: {output_file}")
    logging.info(f"Output formats: {args.output_formats}")
    
    try:
        # Load teams data
        if not os.path.exists(teams_file):
            logging.error(f"Teams file not found: {teams_file}")
            print(f"❌ Teams file not found: {teams_file}")
            print("Please run team_id_mapper.py first to generate the teams file.")
            return 1
        
        all_teams = load_teams_from_json(teams_file)
        logging.info(f"Loaded {len(all_teams)} teams from {teams_file}")
        
        # Filter teams if specified
        if args.teams:
            filtered_teams = {}
            for team_id, team_data in all_teams.items():
                if team_data['team_name'] in args.teams:
                    filtered_teams[team_id] = team_data
            all_teams = filtered_teams
            logging.info(f"Filtered to {len(all_teams)} teams: {args.teams}")
        
        # Filter seasons if specified
        if args.seasons:
            for team_data in all_teams.values():
                team_data['seasons'] = [s for s in team_data['seasons'] if s in args.seasons]
            logging.info(f"Filtered to seasons: {args.seasons}")
        
        if not all_teams:
            logging.error("No teams to process after filtering")
            return 1
        
        # Extract fixtures
        fixtures_data = extract_all_team_fixtures(
            all_teams, 
            progress_save=args.progress_save,
            output_dir=os.path.dirname(output_file)
        )
        
        if fixtures_data:
            # Save raw fixtures data
            save_json_data(fixtures_data, output_file)
            
            # Convert to DataFrame and save in requested formats
            fixtures_df = fixtures_data_to_dataframe(fixtures_data)
            
            if not fixtures_df.empty:
                save_dataframe_to_multiple_formats(
                    fixtures_df, 
                    f"{output_file}_dataframe",
                    formats=args.output_formats
                )
                
                # Print summary
                print(f"\n✅ Successfully extracted fixtures for {len(fixtures_data)} teams")
                print(f"📊 Total matches: {len(fixtures_df)}")
                print(f"🌍 Environment: {args.environment}")
                print(f"📁 Raw data saved to: {output_file}.json")
                if 'json' in args.output_formats:
                    print(f"📁 DataFrame saved to: {output_file}_dataframe.json")
                if 'csv' in args.output_formats:
                    print(f"📁 DataFrame saved to: {output_file}_dataframe.csv")
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