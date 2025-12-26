#!/usr/bin/env python

import subprocess    # for running external programs/scripts
import sys
import logging
import os
from pathlib import Path
from datetime import datetime


# logging to both file and console
log_dir = Path(__file__).parent / 'logs'
log_dir.mkdir(exist_ok=True)

log_filename = f'hh_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
log_filepath = log_dir / log_filename


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath, encoding='utf-8'),   # to file
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)   


def run_script(script_path, description, vacancy_name):
    try:
        result = subprocess.run(
            ['python', str(script_path), vacancy_name],
            check=True, capture_output=True, text=True, cwd='.'
        )
        logger.info(f'{description}')
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f'{description}: {e.stderr}')
        return False
        
    except subprocess.CalledProcessError as e:
        logger.error(f'Error in {script_path.name}: {e.stderr}')
        return False

def main(vacancy_name):
    
    scripts = [
        ('scripts/01_fetch_data/fetch_vacancies.py', 'Fetching vacancies'),
        ('scripts/02_convert_to_csv/json_to_csv.py', 'Converting to CSV'),
        ('scripts/03_sort_data/sorter.py', 'Sorting data'),
        ('scripts/04_change_data/cleaner.py', 'Enriching data'),
        ('scripts/05_aggregate_data/counter.py', 'Aggregating data'),
        ('scripts/06_partition_data/partitioner.py', 'Partitioning'),
        ('scripts/06_partition_data/concatenator.py', 'Concatenating')
    ]
    
    logger.info(f'Starting ETL pipeline for: "{vacancy_name}"')
    
    for script_rel_path, description in scripts:
        script_path = Path(script_rel_path)
        
        if not run_script(script_path, description, vacancy_name):
            logger.error('Pipeline interrupted')
            return False
    
    logger.info('Pipeline completed successfully!')
    return True

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python run_etl.py "job title"')
        sys.exit(1)
    
    success = main(sys.argv[1])
    sys.exit(0 if success else 1)