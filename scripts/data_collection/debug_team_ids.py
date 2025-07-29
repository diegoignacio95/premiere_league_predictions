import requests
from bs4 import BeautifulSoup
import re
import time
import random

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def debug_page_structure(season='2023-2024'):
    """Debug the FBRef page structure to find tables and team links"""
    url = f"https://fbref.com/en/comps/9/{season}/{season}-Premier-League-Stats"
    print(f"URL: {url}")
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all tables
    tables = soup.find_all('table')
    print(f"\nTotal tables found: {len(tables)}")
    
    for i, table in enumerate(tables):
        table_id = table.get('id', 'No ID')
        table_class = table.get('class', 'No class')
        print(f"Table {i}: ID = '{table_id}', Class = {table_class}")
    
    # Look for team links specifically
    print(f"\n--- Looking for team links ---")
    team_links = soup.find_all('a', href=re.compile(r'/en/squads/[a-f0-9]+/'))
    print(f"Found {len(team_links)} team links")
    
    team_mapping = {}
    for link in team_links[:10]:  # Show first 10
        href = link.get('href')
        team_name = link.text.strip()
        
        # Extract team ID
        team_id_match = re.search(r'/squads/([a-f0-9]+)/', href)
        if team_id_match and team_name:
            team_id = team_id_match.group(1)
            if team_name not in team_mapping:  # Avoid duplicates
                team_mapping[team_name] = team_id
                print(f"  {team_name}: {team_id}")
    
    return soup, team_mapping

# Run the debug function
if __name__ == "__main__":
    soup, teams = debug_page_structure('2023-2024')
    print(f"\nFound {len(teams)} unique teams")