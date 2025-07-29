"""
Data processing utilities for the Pronósticos Football project.

This module contains functions for loading, saving, and transforming
data between different formats (JSON, DataFrame, etc.).
"""

import json
import pandas as pd
import os
from typing import Dict, Any, Optional
from datetime import datetime


def load_teams_from_json(json_filename: str) -> Dict[str, Any]:
    """
    Load teams data from JSON file.
    
    Args:
        json_filename: Path to JSON file
    
    Returns:
        Teams data dictionary
    """
    with open(json_filename, 'r', encoding='utf-8') as f:
        teams_data = json.load(f)
    return teams_data


def save_json_data(data: Dict[str, Any], filename: str, output_dir: str = None) -> None:
    """
    Save data to JSON file with proper serialization.
    
    Args:
        data: Data to save
        filename: Name of output file (without extension)
        output_dir: Directory to save file (optional)
    """
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"{filename}.json")
    else:
        filepath = f"{filename}.json"
    
    # Convert sets to lists for JSON serialization if needed
    serializable_data = _make_json_serializable(data)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(serializable_data, f, indent=2, ensure_ascii=False)
    
    print(f"Data saved to {filepath}")


def load_json_data(json_filename: str) -> Dict[str, Any]:
    """
    Load data from JSON file.
    
    Args:
        json_filename: Path to JSON file
    
    Returns:
        Loaded data dictionary
    """
    with open(json_filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def _make_json_serializable(obj: Any) -> Any:
    """
    Recursively convert objects to JSON-serializable format.
    
    Args:
        obj: Object to convert
        
    Returns:
        JSON-serializable object
    """
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {key: _make_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_make_json_serializable(item) for item in obj]
    else:
        return obj


def fixtures_data_to_dataframe(fixtures_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert fixtures data dictionary to a pandas DataFrame.
    
    Args:
        fixtures_data: Fixtures data from extract_all_team_fixtures()
    
    Returns:
        Flattened DataFrame with one row per match
    """
    all_records = []
    
    for team_id, team_data in fixtures_data.items():
        team_name = team_data['team_name']
        
        for season, season_data in team_data['seasons_data'].items():
            if season_data and season_data.get('matches'):
                
                for match in season_data['matches']:
                    record = {
                        'team_id': team_id,
                        'team_name': team_name,
                        'season': season
                    }
                    record.update(match)
                    all_records.append(record)
    
    df = pd.DataFrame(all_records)
    
    if len(df) > 0:
        # Add full match report URL if match_report_href exists
        if 'match_report_href' in df.columns:
            df['full_match_report_url'] = 'https://fbref.com' + df['match_report_href']
        
        # Reorder columns for better readability
        team_columns = ['team_name', 'season', 'team_id']
        fixtures_columns = [
            'date', 'time', 'comp', 'round', 'day', 'venue', 'result', 
            'gf', 'ga', 'opponent', 'xg', 'xga', 'poss', 
            'attendance', 'captain', 'formation', 'formation_opp', 
            'referee', 'match_report', 'notes'
        ]
        
        available_team_cols = [col for col in team_columns if col in df.columns]
        available_fixtures_cols = [col for col in fixtures_columns if col in df.columns]
        other_columns = [col for col in df.columns if col not in team_columns + fixtures_columns]
        
        final_columns = available_team_cols + available_fixtures_cols + other_columns
        df = df[final_columns]
    
    return df


def wages_data_to_dataframe(wages_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert wages data dictionary to a pandas DataFrame.
    
    Args:
        wages_data: Wages data from extract_all_team_wages()
    
    Returns:
        Flattened DataFrame with one row per player per season
    """
    all_records = []
    
    for team_id, team_data in wages_data.items():
        team_name = team_data['team_name']
        
        for season, season_data in team_data['seasons_data'].items():
            if season_data and season_data.get('players'):
                
                for player in season_data['players']:
                    record = {
                        'team_id': team_id,
                        'team_name': team_name,
                        'season': season,
                        'tables_found': ', '.join(season_data.get('tables_found', [])),
                    }
                    record.update(player)
                    all_records.append(record)
    
    df = pd.DataFrame(all_records)
    
    # Reorder columns for better readability
    if len(df) > 0:
        priority_columns = ['team_name', 'season', 'player_name', 'age', 'annual_wages', 'weekly_wages']
        other_columns = [col for col in df.columns if col not in priority_columns]
        
        available_priority_cols = [col for col in priority_columns if col in df.columns]
        df = df[available_priority_cols + other_columns]
    
    return df


def match_stats_to_dataframe(stats_list: list) -> pd.DataFrame:
    """
    Convert list of match stats to DataFrame.
    
    Args:
        stats_list: List of match statistics dictionaries
    
    Returns:
        DataFrame with match statistics
    """
    if not stats_list:
        return pd.DataFrame()
    
    df = pd.DataFrame(stats_list)
    
    # Reorder columns for better readability
    if len(df) > 0:
        priority_columns = ['match_id', 'team_name', 'stat_name', 'stat_value']
        other_columns = [col for col in df.columns if col not in priority_columns]
        
        available_priority_cols = [col for col in priority_columns if col in df.columns]
        df = df[available_priority_cols + other_columns]
    
    return df


def save_dataframe_to_multiple_formats(df: pd.DataFrame, filename: str, 
                                     output_dir: str = None, 
                                     formats: list = ['json', 'csv']) -> None:
    """
    Save DataFrame to multiple formats.
    
    Args:
        df: DataFrame to save
        filename: Base filename (without extension)
        output_dir: Directory to save files (optional)
        formats: List of formats to save ('json', 'csv', 'parquet')
    """
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    for fmt in formats:
        if output_dir:
            filepath = os.path.join(output_dir, f"{filename}.{fmt}")
        else:
            filepath = f"{filename}.{fmt}"
        
        if fmt == 'json':
            df.to_json(filepath, orient='records', indent=2)
        elif fmt == 'csv':
            df.to_csv(filepath, index=False)
        elif fmt == 'parquet':
            df.to_parquet(filepath, index=False)
        
        print(f"DataFrame saved to {filepath}")


def filter_fixtures_by_criteria(fixtures_df: pd.DataFrame, 
                               teams: Optional[list] = None,
                               seasons: Optional[list] = None,
                               competitions: Optional[list] = None) -> pd.DataFrame:
    """
    Filter fixtures DataFrame by various criteria.
    
    Args:
        fixtures_df: DataFrame with fixtures data
        teams: List of team names to include
        seasons: List of seasons to include
        competitions: List of competitions to include
    
    Returns:
        Filtered DataFrame
    """
    filtered_df = fixtures_df.copy()
    
    if teams:
        filtered_df = filtered_df[filtered_df['team_name'].isin(teams)]
    
    if seasons:
        filtered_df = filtered_df[filtered_df['season'].isin(seasons)]
    
    if competitions:
        filtered_df = filtered_df[filtered_df['comp'].isin(competitions)]
    
    print(f"Filtered to {len(filtered_df)} matches")
    return filtered_df


def get_match_urls_from_fixtures(fixtures_df: pd.DataFrame) -> list:
    """
    Extract match URLs from fixtures DataFrame.
    
    Args:
        fixtures_df: DataFrame with fixtures data
    
    Returns:
        List of unique match URLs
    """
    if 'full_match_report_url' in fixtures_df.columns:
        return fixtures_df['full_match_report_url'].unique().tolist()
    elif 'match_report_href' in fixtures_df.columns:
        return ['https://fbref.com' + href for href in fixtures_df['match_report_href'].unique()]
    else:
        print("No match URLs found in DataFrame")
        return []


def create_progress_filename(base_name: str, count: int, timestamp: bool = False) -> str:
    """
    Create standardized progress filenames.
    
    Args:
        base_name: Base name for the file
        count: Current count/iteration
        timestamp: Whether to include timestamp
    
    Returns:
        Formatted filename
    """
    if timestamp:
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{base_name}_{count}_{timestamp_str}"
    else:
        return f"{base_name}_{count}"