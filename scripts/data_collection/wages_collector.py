#!/usr/bin/env python3
"""
Wages Collection Script

This script extracts player wage information for Premier League teams
from FBRef across multiple seasons.

Based on: notebooks/exploratory/team_wages_scraper.ipynb
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
    wages_data_to_dataframe,
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
            logging.FileHandler('wages_collection.log')
        ]
    )


def extract_team_wages_complete(team_id: str, season: str, team_name: str) -> Dict:
    """
    Extract wages information from both 'wages' and 'div_wages' tables.
    
    Args:
        team_id: FBRef team ID (e.g., 'b8fd03ef')
        season: Season in format '2023-2024'
        team_name: Team name for URL construction
    
    Returns:
        Dictionary containing wages data from both tables
    """
    logging.info(f"Fetching wages for {team_name} ({season})...")
    
    url = create_fbref_url("squads", team_id=team_id, season=season, 
                          team_name=team_name, page_type="wages")
    
    logging.debug(f"URL: {url}")
    
    soup = get_page(url)
    if not soup:
        logging.error(f"Failed to fetch wages page for {team_name} ({season})")
        return {}
    
    # Look for both wages tables
    tables_to_find = ['wages', 'div_wages']
    found_tables = {}
    
    for table_id in tables_to_find:
        table = soup.find('table', {'id': table_id})
        if table:
            found_tables[table_id] = table
            logging.info(f"Found table: {table_id}")
    
    if not found_tables:
        logging.warning(f"No wages tables found for {team_name} ({season})")
        return {}
    
    # Initialize wages data structure
    wages_data = {
        'team_id': team_id,
        'team_name': team_name,
        'season': season,
        'players': [],
        'tables_found': list(found_tables.keys())
    }
    
    # Process each table
    for table_name, table in found_tables.items():
        logging.info(f"Processing table: {table_name}")
        
        tbody = table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
        else:
            rows = table.find_all('tr')
            # Filter out header rows
            rows = [row for row in rows if row.find('td')]
        
        logging.info(f"Found {len(rows)} rows in {table_name}")
        
        for row in rows:
            player_data = {'table_source': table_name}
            
            # Extract player name
            player_cell = row.find('th', {'data-stat': 'player'}) or row.find('td', {'data-stat': 'player'})
            if player_cell:
                player_link = player_cell.find('a')
                player_data['player_name'] = player_link.text.strip() if player_link else player_cell.text.strip()
            
            # Extract all available data columns
            cells = row.find_all(['td', 'th'])
            for cell in cells:
                data_stat = cell.get('data-stat')
                if data_stat and data_stat != 'player':  # Skip player as we already handled it
                    cell_text = cell.text.strip()
                    if cell_text and cell_text != '':
                        player_data[data_stat] = cell_text
            
            # Only add player if we have at least the name
            if player_data.get('player_name'):
                wages_data['players'].append(player_data)
    
    logging.info(f"Extracted {len(wages_data['players'])} player records for {team_name}")
    return wages_data


def extract_all_team_wages(all_teams_dict: Dict[str, Dict], 
                         progress_save: bool = True,
                         output_dir: str = "../../data/raw/") -> Dict:
    """
    Extract wages data for all teams across all seasons they played.
    
    Args:
        all_teams_dict: Dictionary containing team information with seasons
        progress_save: Whether to save progress periodically
        output_dir: Directory for progress saves
        
    Returns:
        Complete wages dataset organized by team_id and season
    """
    all_wages_data = {}
    total_extractions = sum(len(team_info['seasons']) for team_info in all_teams_dict.values())
    current_extraction = 0
    
    logging.info(f"Starting wages extraction for {len(all_teams_dict)} teams "
                f"across {total_extractions} team-season combinations...")
    
    for team_id, team_info in all_teams_dict.items():
        team_name = team_info['team_name']
        seasons = team_info['seasons']
        
        logging.info(f"Processing {team_name} (ID: {team_id})")
        logging.info(f"   Seasons to extract: {seasons}")
        
        # Initialize team entry in results
        if team_id not in all_wages_data:
            all_wages_data[team_id] = {
                'team_name': team_name,
                'team_id': team_id,
                'seasons_data': {}
            }
        
        # Extract wages for each season this team played
        for season in seasons:
            current_extraction += 1
            logging.info(f"[{current_extraction}/{total_extractions}] "
                        f"Extracting {team_name} {season}...")
            
            try:
                season_wages = extract_team_wages_complete(team_id, season, team_name)
                
                if season_wages and season_wages.get('players'):
                    all_wages_data[team_id]['seasons_data'][season] = season_wages
                    player_count = len(season_wages['players'])
                    tables_found = season_wages.get('tables_found', [])
                    logging.info(f"   ✅ Success: {player_count} players, tables: {tables_found}")
                else:
                    logging.warning(f"   ⚠️  No wage data found for {team_name} in {season}")
                    all_wages_data[team_id]['seasons_data'][season] = None
                    
            except Exception as e:
                logging.error(f"   ❌ Error extracting {team_name} {season}: {str(e)}")
                all_wages_data[team_id]['seasons_data'][season] = None
            
            # Small delay to be respectful to the server
            time.sleep(1)
            
            # Save progress every 10 extractions
            if progress_save and current_extraction % 10 == 0:
                progress_file = f"wages_progress_{current_extraction}.json"
                save_json_data(all_wages_data, 
                             os.path.join(output_dir, progress_file.replace('.json', '')))
                logging.info(f"Progress saved at {current_extraction} extractions")
    
    # Summary statistics
    successful_extractions = 0
    total_players = 0
    
    for team_data in all_wages_data.values():
        for season_data in team_data['seasons_data'].values():
            if season_data and season_data.get('players'):
                successful_extractions += 1
                total_players += len(season_data['players'])
    
    logging.info(f"Extraction Summary:")
    logging.info(f"  Total teams processed: {len(all_wages_data)}")
    logging.info(f"  Successful extractions: {successful_extractions}/{total_extractions}")
    logging.info(f"  Total player records: {total_players}")
    
    return all_wages_data


def calculate_wage_summary(wages_data: Dict) -> Dict:
    """
    Calculate summary statistics for extracted wage data.
    
    Args:
        wages_data: Complete wages dataset
        
    Returns:
        Dictionary with summary statistics
    """
    summary = {
        'total_teams': len(wages_data),
        'total_seasons': 0,
        'total_players': 0,
        'teams_by_season': {},
        'players_by_team': {},
        'tables_coverage': {'wages': 0, 'div_wages': 0}
    }
    
    for team_id, team_data in wages_data.items():
        team_name = team_data['team_name']
        summary['players_by_team'][team_name] = 0
        
        for season, season_data in team_data['seasons_data'].items():
            if season_data and season_data.get('players'):
                summary['total_seasons'] += 1
                
                # Count players
                player_count = len(season_data['players'])
                summary['total_players'] += player_count
                summary['players_by_team'][team_name] += player_count
                
                # Track seasons
                if season not in summary['teams_by_season']:
                    summary['teams_by_season'][season] = 0
                summary['teams_by_season'][season] += 1
                
                # Track table coverage
                tables_found = season_data.get('tables_found', [])
                if 'wages' in tables_found:
                    summary['tables_coverage']['wages'] += 1
                if 'div_wages' in tables_found:
                    summary['tables_coverage']['div_wages'] += 1
    
    return summary


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Extract Premier League wages from FBRef')
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
        '--summary',
        action='store_true',
        help='Generate and save summary statistics'
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
        output_file = f'../../data/{args.environment}/raw/premier_league_wages'
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info("Starting Premier League wages collection")
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
        
        # Extract wages
        wages_data = extract_all_team_wages(
            all_teams, 
            progress_save=args.progress_save,
            output_dir=os.path.dirname(output_file)
        )
        
        if wages_data:
            # Save raw wages data
            save_json_data(wages_data, output_file)
            
            # Convert to DataFrame and save in requested formats
            wages_df = wages_data_to_dataframe(wages_data)
            
            if not wages_df.empty:
                save_dataframe_to_multiple_formats(
                    wages_df, 
                    f"{output_file}_dataframe",
                    formats=args.output_formats
                )
                
                # Generate summary if requested
                if args.summary:
                    summary = calculate_wage_summary(wages_data)
                    save_json_data(summary, f"{output_file}_summary")
                    
                    print(f"\n📊 Wage Collection Summary:")
                    print(f"   Teams processed: {summary['total_teams']}")
                    print(f"   Total seasons: {summary['total_seasons']}")
                    print(f"   Total players: {summary['total_players']}")
                    print(f"   Average players per team: {summary['total_players'] / summary['total_teams']:.1f}")
                
                # Print final summary
                print(f"\n✅ Successfully extracted wages for {len(wages_data)} teams")
                print(f"📊 Total player records: {len(wages_df)}")
                print(f"🌍 Environment: {args.environment}")
                print(f"📁 Raw data saved to: {output_file}.json")
                if 'json' in args.output_formats:
                    print(f"📁 DataFrame saved to: {output_file}_dataframe.json")
                if 'csv' in args.output_formats:
                    print(f"📁 DataFrame saved to: {output_file}_dataframe.csv")
            else:
                logging.warning("No wages data was converted to DataFrame")
        else:
            logging.error("No wages were extracted")
            return 1
            
    except Exception as e:
        logging.error(f"Wages collection failed: {str(e)}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())