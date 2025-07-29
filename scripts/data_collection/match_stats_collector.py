#!/usr/bin/env python3
"""
Match Stats Collection Script

This script extracts detailed match statistics from FBRef match pages
in a scalable way with anti-blocking measures.

Based on: notebooks/exploratory/match_stats_scraper_scaled.ipynb
"""

import sys
import os
from typing import Dict, List, Optional, Tuple
import argparse
import logging
import time
import json
from datetime import datetime

# Add the scripts directory to the path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.scraping_utils import get_page, EnhancedScraper
from utils.data_utils import (
    load_teams_from_json, 
    save_json_data, 
    fixtures_data_to_dataframe,
    get_match_urls_from_fixtures,
    match_stats_to_dataframe,
    save_dataframe_to_multiple_formats,
    create_progress_filename
)
from utils.text_utils import extract_percentage_or_value


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('match_stats_collection.log')
        ]
    )


def scrape_team_stats(soup, match_id: str = None) -> Optional[Tuple[List[Dict], str, str]]:
    """
    Scrape main team statistics from match page in long format.
    
    Args:
        soup: BeautifulSoup object of the match page
        match_id: Match identifier (URL)
    
    Returns:
        Tuple of (stats_list, team1_name, team2_name) or None
    """
    team_stats_div = soup.find('div', {'id': 'team_stats'})
    
    if not team_stats_div:
        logging.warning("No team_stats div found")
        return None
    
    table = team_stats_div.find('table')
    if not table:
        logging.warning("No table found in team_stats div")
        return None
    
    # Extract team names from header
    header_row = table.find('tr')
    team_cells = header_row.find_all('th')
    team1_name = team_cells[0].get_text(strip=True).split()[0]
    team2_name = team_cells[1].get_text(strip=True).split()[-1]
    
    # Parse stats in long format
    stats_data = []
    rows = table.find_all('tr')[1:]  # Skip header
    
    i = 0
    while i < len(rows):
        # Each stat has a header row followed by a data row
        if i + 1 < len(rows):
            header_row = rows[i]
            data_row = rows[i + 1]
            
            # Get stat name
            stat_name = header_row.get_text(strip=True)
            
            if stat_name and stat_name != "Cards":
                # Get values for both teams
                data_cells = data_row.find_all('td')
                if len(data_cells) == 2:
                    team1_value = data_cells[0].get_text(strip=True)
                    team2_value = data_cells[1].get_text(strip=True)
                    
                    # Use improved extraction function
                    team1_clean = extract_percentage_or_value(team1_value)
                    team2_clean = extract_percentage_or_value(team2_value)
                    
                    # Add two rows: one for each team (long format)
                    stats_data.append({
                        'match_id': match_id,
                        'team_name': team1_name,
                        'stat_name': stat_name,
                        'stat_value': team1_clean
                    })
                    stats_data.append({
                        'match_id': match_id,
                        'team_name': team2_name,
                        'stat_name': stat_name,
                        'stat_value': team2_clean
                    })
        
        i += 2  # Skip to next stat (header + data)
    
    return stats_data, team1_name, team2_name


def scrape_team_stats_extra(soup, team1_name: str, team2_name: str, match_id: str = None) -> Optional[List[Dict]]:
    """
    Scrape extra team statistics from match page in long format.
    
    Args:
        soup: BeautifulSoup object of the match page
        team1_name: Name of first team
        team2_name: Name of second team
        match_id: Match identifier (URL)
    
    Returns:
        List of stats dictionaries or None
    """
    team_stats_extra_div = soup.find('div', {'id': 'team_stats_extra'})
    
    if not team_stats_extra_div:
        logging.warning("No team_stats_extra div found")
        return None
    
    stats_data = []
    
    # Find all stat containers
    stat_containers = team_stats_extra_div.find_all('div', recursive=False)
    
    for container in stat_containers:
        divs = container.find_all('div')
        if len(divs) >= 3:
            # Each row has: team1_value, stat_name, team2_value pattern
            for i in range(0, len(divs), 3):
                if i + 2 < len(divs):
                    team1_value = divs[i].get_text(strip=True)
                    stat_name = divs[i + 1].get_text(strip=True)
                    team2_value = divs[i + 2].get_text(strip=True)
                    
                    # Skip headers and invalid data
                    if team1_value.isdigit() and team2_value.isdigit():
                        # Add two rows: one for each team (long format)
                        stats_data.append({
                            'match_id': match_id,
                            'team_name': team1_name,
                            'stat_name': stat_name,
                            'stat_value': team1_value
                        })
                        stats_data.append({
                            'match_id': match_id,
                            'team_name': team2_name,
                            'stat_name': stat_name,
                            'stat_value': team2_value
                        })
    
    return stats_data if stats_data else None


def scrape_match_stats(match_url: str, scraper=None) -> Optional[List[Dict]]:
    """
    Scrape all team stats (main + extra) from a single match URL in long format.
    
    Args:
        match_url: URL of the match page
        scraper: Optional EnhancedScraper instance
    
    Returns:
        Combined list with all team stats in long format or None
    """
    logging.info(f"Scraping: {match_url}")
    
    # Fetch the page
    if scraper:
        soup = scraper.get_page_enhanced(match_url)
    else:
        soup = get_page(match_url)
    
    if not soup:
        logging.error(f"Failed to fetch page: {match_url}")
        return None
    
    # Scrape team stats
    team_stats_result = scrape_team_stats(soup, match_url)
    if team_stats_result is None:
        logging.warning("Failed to scrape team stats")
        return None
    
    team_stats_list, team1_name, team2_name = team_stats_result
    
    # Scrape team stats extra
    team_stats_extra_list = scrape_team_stats_extra(soup, team1_name, team2_name, match_url)
    
    # Combine the lists
    all_stats = []
    if team_stats_list:
        all_stats.extend(team_stats_list)
        logging.info(f"Found {len(team_stats_list)} main stats rows")
    
    if team_stats_extra_list:
        all_stats.extend(team_stats_extra_list)
        logging.info(f"Found {len(team_stats_extra_list)} extra stats rows")
    
    if all_stats:
        logging.info(f"Total stats collected: {len(all_stats)} rows")
        return all_stats
    else:
        logging.warning("No stats available")
        return None


def scrape_multiple_matches(match_urls: List[str], 
                          max_matches: Optional[int] = None,
                          use_enhanced_scraper: bool = True,
                          save_progress: bool = True,
                          output_dir: str = '../../data/raw/match_stats/') -> List[Dict]:
    """
    Scrape stats from multiple matches with progress saving.
    
    Args:
        match_urls: List of match URLs to scrape
        max_matches: Maximum number of matches to process (None for all)
        use_enhanced_scraper: Whether to use enhanced anti-blocking scraper
        save_progress: Whether to save progress periodically
        output_dir: Directory to save progress files
    
    Returns:
        Combined list with all match stats
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    all_stats = []
    failed_urls = []
    
    # Limit matches if specified
    urls_to_process = match_urls[:max_matches] if max_matches else match_urls
    
    logging.info(f"Processing {len(urls_to_process)} matches...")
    
    # Setup scraper if enhanced mode is requested
    scraper = None
    if use_enhanced_scraper:
        scraper = EnhancedScraper(
            min_delay=3.0,      # Conservative delays
            max_delay=12.0,
            chunk_size=50,      # Small chunks
            chunk_break=300.0   # 5 minute breaks
        )
        logging.info("Using enhanced scraper with anti-blocking measures")
    
    start_time = time.time()
    
    for i, match_url in enumerate(urls_to_process, 1):
        logging.info(f"[{i}/{len(urls_to_process)}] Processing match...")
        
        try:
            stats_list = scrape_match_stats(match_url, scraper)
            
            if stats_list:
                all_stats.extend(stats_list)
                logging.info(f"✅ Successfully scraped {len(stats_list)} stats")
            else:
                failed_urls.append(match_url)
                logging.warning(f"❌ Failed to scrape stats")
        
        except Exception as e:
            logging.error(f"❌ Error processing {match_url}: {e}")
            failed_urls.append(match_url)
        
        # Save progress every 20 matches
        if save_progress and i % 20 == 0 and all_stats:
            progress_filename = create_progress_filename("match_stats_progress", i)
            progress_file = os.path.join(output_dir, f"{progress_filename}.json")
            
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump(all_stats, f, indent=2, ensure_ascii=False)
            
            elapsed = time.time() - start_time
            rate = i / elapsed * 3600  # matches per hour
            logging.info(f"💾 Progress saved: {progress_file}")
            logging.info(f"📈 Rate: {rate:.1f} matches/hour")
    
    # Final results
    if all_stats:
        elapsed = time.time() - start_time
        total_time = elapsed / 3600  # hours
        successful_matches = len(urls_to_process) - len(failed_urls)
        
        logging.info(f"🎉 SCRAPING COMPLETE!")
        logging.info(f"✅ Successfully processed: {successful_matches} matches")
        logging.info(f"❌ Failed to process: {len(failed_urls)} matches")
        logging.info(f"📊 Total stats collected: {len(all_stats)} rows")
        logging.info(f"⏰ Total time: {total_time:.2f} hours")
        logging.info(f"📈 Average rate: {successful_matches/total_time:.1f} matches/hour")
        
        # Save failed URLs
        if failed_urls:
            failed_file = os.path.join(output_dir, f"failed_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(failed_urls, f, indent=2)
            logging.info(f"📝 Failed URLs saved: {failed_file}")
        
        return all_stats
    else:
        logging.error("No matches were successfully processed")
        return []


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Extract match statistics from FBRef')
    parser.add_argument(
        '--environment',
        default='prod',
        choices=['dev', 'prod'],
        help='Data environment (default: prod)'
    )
    parser.add_argument(
        '--fixtures-file',
        help='Path to fixtures JSON file (optional, will use environment-based path if not specified)'
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
        '--max-matches',
        type=int,
        help='Maximum number of matches to process (for testing)'
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
        '--competitions',
        nargs='+',
        default=['Premier League'],
        help='Competitions to include (default: Premier League)'
    )
    parser.add_argument(
        '--enhanced-scraper',
        action='store_true',
        default=True,
        help='Use enhanced scraper with anti-blocking measures (default: True)'
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
    if args.fixtures_file:
        fixtures_file = args.fixtures_file
    else:
        fixtures_file = f'../../data/{args.environment}/raw/all_competitions_fixtures.json'
    
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = f'../../data/{args.environment}/raw/match_stats/all_match_stats'
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info("Starting match statistics collection")
    logging.info(f"Environment: {args.environment}")
    logging.info(f"Fixtures file: {fixtures_file}")
    logging.info(f"Output file prefix: {output_file}")
    logging.info(f"Output formats: {args.output_formats}")
    logging.info(f"Enhanced scraper: {args.enhanced_scraper}")
    
    try:
        # Load fixtures data
        if not os.path.exists(fixtures_file):
            logging.error(f"Fixtures file not found: {fixtures_file}")
            print(f"❌ Fixtures file not found: {fixtures_file}")
            print("Please run fixtures_collector.py first to generate the fixtures file.")
            return 1
        
        # Load fixtures and convert to DataFrame for filtering
        with open(fixtures_file, 'r', encoding='utf-8') as f:
            fixtures_data = json.load(f)
        
        fixtures_df = fixtures_data_to_dataframe(fixtures_data)
        logging.info(f"Loaded {len(fixtures_df)} fixtures from {fixtures_file}")
        
        # Apply filters
        if args.teams:
            fixtures_df = fixtures_df[fixtures_df['team_name'].isin(args.teams)]
            logging.info(f"Filtered to teams: {args.teams}")
        
        if args.seasons:
            fixtures_df = fixtures_df[fixtures_df['season'].isin(args.seasons)]
            logging.info(f"Filtered to seasons: {args.seasons}")
        
        if args.competitions:
            fixtures_df = fixtures_df[fixtures_df['comp'].isin(args.competitions)]
            logging.info(f"Filtered to competitions: {args.competitions}")
        
        # Get match URLs
        match_urls = get_match_urls_from_fixtures(fixtures_df)
        logging.info(f"Found {len(match_urls)} unique match URLs to process")
        
        if not match_urls:
            logging.error("No matches to process after filtering")
            return 1
        
        # Extract match statistics
        all_stats = scrape_multiple_matches(
            match_urls,
            max_matches=args.max_matches,
            use_enhanced_scraper=args.enhanced_scraper,
            save_progress=args.progress_save,
            output_dir=os.path.dirname(output_file)
        )
        
        if all_stats:
            # Save raw match stats data
            with open(f"{output_file}.json", 'w', encoding='utf-8') as f:
                json.dump(all_stats, f, indent=2, ensure_ascii=False)
            
            # Convert to DataFrame and save in requested formats
            stats_df = match_stats_to_dataframe(all_stats)
            
            if not stats_df.empty:
                save_dataframe_to_multiple_formats(
                    stats_df, 
                    f"{output_file}_dataframe",
                    formats=args.output_formats
                )
                
                # Print final summary
                print(f"\n✅ Successfully extracted statistics from {len(set(s['match_id'] for s in all_stats))} matches")
                print(f"📊 Total stat records: {len(all_stats)}")
                print(f"🌍 Environment: {args.environment}")
                print(f"📁 Raw data saved to: {output_file}.json")
                if 'json' in args.output_formats:
                    print(f"📁 DataFrame saved to: {output_file}_dataframe.json")
                if 'csv' in args.output_formats:
                    print(f"📁 DataFrame saved to: {output_file}_dataframe.csv")
            else:
                logging.warning("No match stats data was converted to DataFrame")
        else:
            logging.error("No match statistics were extracted")
            return 1
            
    except Exception as e:
        logging.error(f"Match stats collection failed: {str(e)}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())