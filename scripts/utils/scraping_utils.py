"""
Web scraping utilities for FBRef data collection.

This module contains common functions and classes for web scraping
with proper rate limiting, error handling, and anti-blocking measures.
"""

import requests
from bs4 import BeautifulSoup
import time
import random
from typing import Optional, Tuple, Dict, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Multiple User-Agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]


def get_page(url: str, delay_range: Tuple[float, float] = (2, 4), headers: Dict = None) -> Optional[BeautifulSoup]:
    """
    Fetch page with error handling and rate limiting.
    
    Args:
        url: URL to fetch
        delay_range: Tuple of (min_delay, max_delay) in seconds
        headers: Custom headers to use (optional)
    
    Returns:
        BeautifulSoup object or None if failed
    """
    time.sleep(random.uniform(*delay_range))
    
    if headers is None:
        headers = DEFAULT_HEADERS
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


class EnhancedScraper:
    """Enhanced scraper with anti-blocking measures."""
    
    def __init__(self, 
                 min_delay: float = 15.0,
                 max_delay: float = 30.0,
                 max_retries: int = 3,
                 backoff_factor: float = 2.0,
                 chunk_size: int = 50,
                 chunk_break: float = 300.0):
        """
        Initialize enhanced scraper.
        
        Args:
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds) 
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier
            chunk_size: Number of requests per chunk before long break
            chunk_break: Break time between chunks (seconds)
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.chunk_size = chunk_size
        self.chunk_break = chunk_break
        
        self.request_count = 0
        self.session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=self.backoff_factor,
            respect_retry_after_header=True
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_random_headers(self) -> Dict[str, str]:
        """Get random headers for request."""
        return {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def _wait_between_requests(self):
        """Handle delays and chunking."""
        delay = random.uniform(self.min_delay, self.max_delay)
        print(f"⏱️  Waiting {delay:.1f} seconds...")
        time.sleep(delay)
        
        self.request_count += 1
        
        if self.request_count % self.chunk_size == 0:
            print(f"\n🛑 Chunk break after {self.request_count} requests")
            print(f"⏰ Waiting {self.chunk_break/60:.1f} minutes before continuing...")
            time.sleep(self.chunk_break)
            print("🚀 Resuming scraping...\n")
    
    def get_page_enhanced(self, url: str) -> Optional[BeautifulSoup]:
        """
        Enhanced page fetching with anti-blocking measures.
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if failed
        """
        self._wait_between_requests()
        
        headers = self._get_random_headers()
        
        try:
            response = self.session.get(url, headers=headers, timeout=30)
            
            if response.status_code == 429:
                print(f"⚠️  Rate limited. Waiting longer...")
                time.sleep(60)
                return None
            
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
            
        except requests.RequestException as e:
            print(f"❌ Error fetching {url}: {e}")
            
            if "429" in str(e) or "rate" in str(e).lower():
                wait_time = 60 * (2 ** (self.request_count % 3))
                print(f"⏰ Rate limited. Waiting {wait_time/60:.1f} minutes...")
                time.sleep(wait_time)
            
            return None


def create_fbref_url(base_path: str, team_id: str = None, season: str = None, 
                    team_name: str = None, page_type: str = "stats") -> str:
    """
    Create FBRef URLs following their standard patterns.
    
    Args:
        base_path: Base URL path (e.g., "squads", "comps/9")
        team_id: FBRef team ID
        season: Season in format "2023-2024"
        team_name: Team name for URL (will be formatted)
        page_type: Type of page ("stats", "wages", "fixtures", etc.)
    
    Returns:
        Complete FBRef URL
    """
    base_url = "https://fbref.com/en"
    
    if team_id and season and team_name:
        # Format team name for URL
        team_name_url = team_name.replace(' ', '-').replace("'", "")
        if page_type == "wages":
            return f"{base_url}/squads/{team_id}/{season}/wages/{team_name_url}-Wage-Details"
        elif page_type == "fixtures":
            return f"{base_url}/squads/{team_id}/{season}/all_comps/{team_name_url}-Stats-All-Competitions"
        else:
            return f"{base_url}/squads/{team_id}/{season}/{team_name_url}-Stats"
    
    # For league tables, competitions, etc.
    if season and base_path.startswith("comps"):
        return f"{base_url}/{base_path}/{season}/{season}-Premier-League-Stats"
    
    return f"{base_url}/{base_path}"