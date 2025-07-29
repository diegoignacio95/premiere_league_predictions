#!/usr/bin/env python3
"""
Data Collection Orchestrator

This script runs all data collection scripts in the correct order
to collect comprehensive football data from FBRef.

Usage:
    python run_all_collectors.py [options]
"""

import sys
import os
import subprocess
import argparse
import logging
from datetime import datetime

# Add the scripts directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('data_collection_orchestrator.log')
        ]
    )


def run_script(script_name: str, args: list = None, required: bool = True) -> bool:
    """
    Run a data collection script.
    
    Args:
        script_name: Name of the script to run
        args: Additional arguments for the script
        required: Whether this script is required for the pipeline
        
    Returns:
        True if successful, False otherwise
    """
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    
    logging.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info(f"✅ {script_name} completed successfully")
        logging.debug(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ {script_name} failed with return code {e.returncode}")
        logging.error(f"Error output: {e.stderr}")
        if required:
            raise
        return False
    except Exception as e:
        logging.error(f"❌ Unexpected error running {script_name}: {e}")
        if required:
            raise
        return False


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Run all data collection scripts')
    parser.add_argument(
        '--environment',
        default='prod',
        choices=['dev', 'prod'],
        help='Data environment (default: prod)'
    )
    parser.add_argument(
        '--seasons',
        nargs='+',
        default=['2019-2020', '2020-2021', '2021-2022', '2022-2023', '2023-2024', '2024-2025'],
        help='Seasons to process (default: 2019-2020 to 2024-2025)'
    )
    parser.add_argument(
        '--teams',
        nargs='+',
        help='Specific teams to process (optional)'
    )
    parser.add_argument(
        '--skip-team-mapping',
        action='store_true',
        help='Skip team ID mapping (use existing file)'
    )
    parser.add_argument(
        '--skip-fixtures',
        action='store_true',
        help='Skip fixtures collection'
    )
    parser.add_argument(
        '--skip-wages',
        action='store_true',
        help='Skip wages collection'
    )
    parser.add_argument(
        '--skip-match-stats',
        action='store_true',
        help='Skip match statistics collection'
    )
    parser.add_argument(
        '--output-formats',
        nargs='+',
        default=['json', 'csv'],
        choices=['json', 'csv', 'parquet'],
        help='Output formats (default: json, csv)'
    )
    parser.add_argument(
        '--max-matches',
        type=int,
        help='Maximum matches for stats collection (for testing)'
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
    
    start_time = datetime.now()
    logging.info("=" * 80)
    logging.info("Starting complete data collection pipeline")
    logging.info(f"Start time: {start_time}")
    logging.info(f"Environment: {args.environment}")
    logging.info(f"Seasons: {args.seasons}")
    if args.teams:
        logging.info(f"Teams filter: {args.teams}")
    logging.info("=" * 80)
    
    try:
        success_count = 0
        total_scripts = 0
        
        # Step 1: Team ID Mapping
        if not args.skip_team_mapping:
            total_scripts += 1
            logging.info("\n🏗️  Step 1: Team ID Mapping")
            team_args = ['--environment', args.environment, '--seasons'] + args.seasons
            if run_script('team_id_mapper.py', team_args):
                success_count += 1
        else:
            logging.info("\n⏭️  Skipping Step 1: Team ID Mapping")
        
        # Step 2: Fixtures Collection
        if not args.skip_fixtures:
            total_scripts += 1
            logging.info("\n🏗️  Step 2: Fixtures Collection")
            fixtures_args = ['--environment', args.environment, '--output-formats'] + args.output_formats
            if args.seasons:
                fixtures_args.extend(['--seasons'] + args.seasons)
            if args.teams:
                fixtures_args.extend(['--teams'] + args.teams)
            if run_script('fixtures_collector.py', fixtures_args):
                success_count += 1
        else:
            logging.info("\n⏭️  Skipping Step 2: Fixtures Collection")
        
        # Step 3: Wages Collection
        if not args.skip_wages:
            total_scripts += 1
            logging.info("\n🏗️  Step 3: Wages Collection")
            wages_args = ['--environment', args.environment, '--output-formats'] + args.output_formats + ['--summary']
            if args.seasons:
                wages_args.extend(['--seasons'] + args.seasons)
            if args.teams:
                wages_args.extend(['--teams'] + args.teams)
            if run_script('wages_collector.py', wages_args, required=False):
                success_count += 1
        else:
            logging.info("\n⏭️  Skipping Step 3: Wages Collection")
        
        # Step 4: Match Statistics Collection
        if not args.skip_match_stats:
            total_scripts += 1
            logging.info("\n🏗️  Step 4: Match Statistics Collection")
            stats_args = ['--environment', args.environment, '--output-formats'] + args.output_formats
            if args.seasons:
                stats_args.extend(['--seasons'] + args.seasons)
            if args.teams:
                stats_args.extend(['--teams'] + args.teams)
            if args.max_matches:
                stats_args.extend(['--max-matches', str(args.max_matches)])
            if run_script('match_stats_collector.py', stats_args, required=False):
                success_count += 1
        else:
            logging.info("\n⏭️  Skipping Step 4: Match Statistics Collection")
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info("\n" + "=" * 80)
        logging.info("DATA COLLECTION PIPELINE COMPLETE")
        logging.info(f"End time: {end_time}")
        logging.info(f"Total duration: {duration}")
        logging.info(f"Environment: {args.environment}")
        logging.info(f"Scripts completed successfully: {success_count}/{total_scripts}")
        
        if success_count == total_scripts:
            logging.info("🎉 All data collection scripts completed successfully!")
            print(f"\n🎉 Data collection pipeline completed successfully!")
            print(f"⏰ Total time: {duration}")
            print(f"🌍 Environment: {args.environment}")
            print(f"📊 {success_count}/{total_scripts} scripts completed")
        else:
            logging.warning(f"⚠️  Pipeline completed with {total_scripts - success_count} failed scripts")
            print(f"\n⚠️  Pipeline completed with some failures")
            print(f"⏰ Total time: {duration}")
            print(f"🌍 Environment: {args.environment}")
            print(f"📊 {success_count}/{total_scripts} scripts completed")
            
        logging.info("=" * 80)
        
        return 0 if success_count == total_scripts else 1
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        print(f"\n❌ Data collection pipeline failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())