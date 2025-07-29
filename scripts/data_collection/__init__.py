"""
Data Collection Module

This module contains scripts for collecting football data from various sources,
primarily FBRef.com for Premier League teams and matches.

Scripts:
- team_id_mapper.py: Extract team IDs and create team mappings
- fixtures_collector.py: Collect match fixtures and results
- wages_collector.py: Collect player wage information
- match_stats_collector.py: Collect detailed match statistics

Usage:
    Each script can be run independently with various command-line options.
    Run with --help for detailed usage information.

Example workflow:
    1. python team_id_mapper.py
    2. python fixtures_collector.py
    3. python wages_collector.py
    4. python match_stats_collector.py
"""

__version__ = "1.0.0"
__author__ = "Diego Sanchez"