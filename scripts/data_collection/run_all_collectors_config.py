#!/usr/bin/env python3
"""
Data Collection Orchestrator (Configuration-based)

This script runs all data collection scripts in the correct order
to collect comprehensive football data from FBRef.

Uses YAML configuration files to define all parameters and settings.

Usage:
    python run_all_collectors_config.py --config prod
    python run_all_collectors_config.py --config dev
    python run_all_collectors_config.py --config testing
"""

import sys
import os
import subprocess
import argparse
import logging
from datetime import datetime

# Add the scripts directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import load_config, setup_logging_from_config


def run_script(script_name: str, config_name: str, additional_args: list = None) -> bool:
    """
    Run a data collection script with configuration.
    
    Args:
        script_name: Name of the script to run
        config_name: Configuration name to pass to the script
        additional_args: Additional arguments for the script
        
    Returns:
        True if successful, False otherwise
    """
    cmd = [sys.executable, script_name, '--config', config_name]
    
    if additional_args:
        cmd.extend(additional_args)
    
    logging.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info(f"+ {script_name} completed successfully")
        logging.debug(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"x {script_name} failed with return code {e.returncode}")
        logging.error(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"x Unexpected error running {script_name}: {e}")
        return False


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Run all data collection scripts using configuration')
    parser.add_argument(
        '--config', 
        default='prod',
        help='Configuration to use (prod, dev, testing, or path to config file)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually doing it'
    )
    parser.add_argument(
        '--step',
        choices=['team_mapping', 'fixtures', 'wages', 'match_stats'],
        help='Run only a specific step'
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Skip steps where output files already exist'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Setup logging from configuration
        setup_logging_from_config(config)
        
        start_time = datetime.now()
        logging.info("=" * 80)
        logging.info("Starting configuration-based data collection pipeline")
        logging.info(f"Start time: {start_time}")
        logging.info(f"Configuration: {args.config}")
        logging.info("=" * 80)
        
        # Print configuration summary
        config.print_summary()
        
        if args.dry_run:
            print("\nDRY RUN - Would run the following steps:")
            
            steps = ['team_mapping', 'fixtures', 'wages', 'match_stats']
            if args.step:
                steps = [args.step]
            
            for step in steps:
                if config.is_step_enabled(step):
                    script_name = f"{step.replace('_', '_')}_config.py"
                    if step == 'team_mapping':
                        script_name = "team_id_mapper_config.py"
                    elif step == 'fixtures':
                        script_name = "fixtures_collector_config.py"
                    elif step == 'wages':
                        script_name = "wages_collector_config.py"
                    elif step == 'match_stats':
                        script_name = "match_stats_collector_config.py"
                    
                    print(f"  {step}: {script_name}")
                else:
                    print(f"  {step}: DISABLED")
            return 0
        
        success_count = 0
        total_scripts = 0
        
        # Define the pipeline steps and their corresponding scripts
        pipeline_steps = [
            ('team_mapping', 'team_id_mapper_config.py'),
            ('fixtures', 'fixtures_collector_config.py'),
            ('wages', 'wages_collector_config.py'),
            ('match_stats', 'match_stats_collector_config.py')
        ]
        
        # Filter to specific step if requested
        if args.step:
            pipeline_steps = [(step, script) for step, script in pipeline_steps if step == args.step]
        
        for step_name, script_name in pipeline_steps:
            # Check if step is enabled in configuration
            if not config.is_step_enabled(step_name):
                logging.info(f"\n>>> Skipping {step_name}: disabled in configuration")
                continue
            
            total_scripts += 1
            logging.info(f"\n>>> Step: {step_name.replace('_', ' ').title()}")
            
            # Check if output exists and should skip
            if args.skip_existing or config.should_skip_if_exists(step_name):
                output_files = {
                    'team_mapping': config.get_raw_data_path('all_teams.json'),
                    'fixtures': config.get_raw_data_path('all_competitions_fixtures.json'),
                    'wages': config.get_raw_data_path('premier_league_wages.json'),
                    'match_stats': config.get_raw_data_path('match_stats', 'all_match_stats.json')
                }
                
                output_file = output_files.get(step_name)
                if output_file and os.path.exists(output_file):
                    logging.info(f">>> Output file exists, skipping: {output_file}")
                    success_count += 1
                    continue
            
            # Prepare additional arguments for the script
            additional_args = []
            
            # Run the script
            if run_script(script_name, args.config, additional_args):
                success_count += 1
            else:
                logging.error(f">>> {step_name} failed")
                # For some steps like wages, failure might be acceptable
                if step_name in ['wages']:
                    logging.warning(f">>> Continuing pipeline despite {step_name} failure")
                    success_count += 1  # Count as success for optional steps
                else:
                    logging.error(f">>> Pipeline stopping due to {step_name} failure")
                    break
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info("\n" + "=" * 80)
        logging.info("DATA COLLECTION PIPELINE COMPLETE")
        logging.info(f"End time: {end_time}")
        logging.info(f"Total duration: {duration}")
        logging.info(f"Configuration: {args.config}")
        logging.info(f"Scripts completed successfully: {success_count}/{total_scripts}")
        
        if success_count == total_scripts:
            logging.info("+ All data collection scripts completed successfully!")
            print(f"\n+ Data collection pipeline completed successfully!")
            print(f"  Total time: {duration}")
            print(f"  Environment: {config.environment}")
            print(f"  Scripts: {success_count}/{total_scripts} completed")
        else:
            logging.warning(f"! Pipeline completed with {total_scripts - success_count} failed scripts")
            print(f"\n! Pipeline completed with some failures")
            print(f"  Total time: {duration}")
            print(f"  Environment: {config.environment}")
            print(f"  Scripts: {success_count}/{total_scripts} completed")
            
        logging.info("=" * 80)
        
        return 0 if success_count == total_scripts else 1
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        print(f"\nx Data collection pipeline failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())