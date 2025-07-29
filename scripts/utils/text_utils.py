"""
Text processing utilities for data extraction.

This module contains functions for parsing and extracting specific
data patterns from scraped text content.
"""

import re
from typing import Optional, Union


def extract_percentage_or_value(text: str) -> str:
    """
    Extract percentage first, if not found then extract first number.
    Prioritizes percentage values over other numbers.
    
    Args:
        text: Text to extract from
        
    Returns:
        Extracted percentage or number as string
    """
    if not text or not isinstance(text, str):
        return str(text) if text is not None else ""
    
    # First try to find percentage
    percentage_match = re.search(r'(\d+(?:\.\d+)?%)', text)
    if percentage_match:
        return percentage_match.group(1)
    
    # If no percentage, try to find any number
    number_match = re.search(r'(\d+(?:\.\d+)?)', text)
    if number_match:
        return number_match.group(1)
    
    # If nothing found, return original text
    return text


def extract_team_id_from_href(href: str) -> Optional[str]:
    """
    Extract team ID from FBRef href link.
    
    Args:
        href: FBRef href path
        
    Returns:
        Team ID string or None if not found
    """
    if not href:
        return None
    
    team_id_match = re.search(r'/squads/([a-f0-9]+)/', href)
    if team_id_match:
        return team_id_match.group(1)
    
    return None


def extract_match_id_from_href(href: str) -> Optional[str]:
    """
    Extract match ID from FBRef match href link.
    
    Args:
        href: FBRef match href path
        
    Returns:
        Match ID string or None if not found
    """
    if not href:
        return None
    
    match_id_match = re.search(r'/matches/([a-f0-9]+)/', href)
    if match_id_match:
        return match_id_match.group(1)
    
    return None


def clean_team_name_for_url(team_name: str) -> str:
    """
    Clean team name for use in URLs following FBRef conventions.
    
    Args:
        team_name: Team name to clean
        
    Returns:
        URL-friendly team name
    """
    if not team_name:
        return ""
    
    # Replace spaces with hyphens and remove apostrophes
    cleaned = team_name.replace(' ', '-').replace("'", "")
    
    # Remove other special characters that might cause issues
    cleaned = re.sub(r'[^\w\-]', '', cleaned)
    
    return cleaned


def parse_wage_value(wage_text: str) -> Optional[dict]:
    """
    Parse wage text to extract monetary values in different currencies.
    
    Args:
        wage_text: Text containing wage information
        
    Returns:
        Dictionary with parsed wage values or None
    """
    if not wage_text or not isinstance(wage_text, str):
        return None
    
    # Pattern to match formats like "£ 350,000 (€ 417,398, $425,327)"
    pattern = r'£\s*([\d,]+)(?:\s*\(€\s*([\d,]+),\s*\$\s*([\d,]+)\))?'
    match = re.search(pattern, wage_text)
    
    if match:
        pounds = match.group(1).replace(',', '') if match.group(1) else None
        euros = match.group(2).replace(',', '') if match.group(2) else None
        dollars = match.group(3).replace(',', '') if match.group(3) else None
        
        result = {}
        if pounds:
            result['pounds'] = int(pounds)
        if euros:
            result['euros'] = int(euros)
        if dollars:
            result['dollars'] = int(dollars)
            
        return result if result else None
    
    return None


def parse_attendance(attendance_text: str) -> Optional[int]:
    """
    Parse attendance text to extract numeric value.
    
    Args:
        attendance_text: Text containing attendance information
        
    Returns:
        Attendance as integer or None
    """
    if not attendance_text or not isinstance(attendance_text, str):
        return None
    
    # Remove commas and extract numbers
    cleaned = attendance_text.replace(',', '')
    number_match = re.search(r'(\d+)', cleaned)
    
    if number_match:
        return int(number_match.group(1))
    
    return None


def standardize_team_name(team_name: str, aliases_map: dict = None) -> str:
    """
    Standardize team name using a mapping of aliases.
    
    Args:
        team_name: Team name to standardize
        aliases_map: Dictionary mapping variations to standard names
        
    Returns:
        Standardized team name
    """
    if not team_name:
        return ""
    
    # Default aliases for common variations
    default_aliases = {
        'Manchester Utd': 'Manchester United',
        'Newcastle Utd': 'Newcastle United',
        'Nott\'ham Forest': 'Nottingham Forest',
        'Sheffield Utd': 'Sheffield United',
        'West Brom': 'West Bromwich Albion',
        'Wolves': 'Wolverhampton Wanderers'
    }
    
    aliases = aliases_map if aliases_map else default_aliases
    
    return aliases.get(team_name, team_name)


def extract_season_from_url(url: str) -> Optional[str]:
    """
    Extract season information from FBRef URL.
    
    Args:
        url: FBRef URL
        
    Returns:
        Season string (e.g., "2023-2024") or None
    """
    if not url:
        return None
    
    season_match = re.search(r'(\d{4}-\d{4})', url)
    if season_match:
        return season_match.group(1)
    
    return None


def validate_team_id_format(team_id: str) -> bool:
    """
    Validate that team ID follows FBRef format (8 character hex).
    
    Args:
        team_id: Team ID to validate
        
    Returns:
        True if valid format, False otherwise
    """
    if not team_id or not isinstance(team_id, str):
        return False
    
    # FBRef team IDs are 8-character hexadecimal strings
    pattern = r'^[a-f0-9]{8}$'
    return bool(re.match(pattern, team_id))


def extract_numeric_value(text: str, data_type: str = "int") -> Union[int, float, None]:
    """
    Extract numeric value from text.
    
    Args:
        text: Text containing numeric value
        data_type: Type to return ("int" or "float")
        
    Returns:
        Numeric value or None if not found
    """
    if not text or not isinstance(text, str):
        return None
    
    # Remove common non-numeric characters
    cleaned = re.sub(r'[,\s£€$%]', '', text)
    
    # Extract number
    if data_type == "float":
        match = re.search(r'(\d+\.?\d*)', cleaned)
        if match:
            return float(match.group(1))
    else:
        match = re.search(r'(\d+)', cleaned)
        if match:
            return int(match.group(1))
    
    return None