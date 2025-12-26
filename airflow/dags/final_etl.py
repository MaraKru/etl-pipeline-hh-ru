from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import importlib.util
import logging
from pathlib import Path
import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

def ensure_directories():
    base_path = Path('/tmp/airflow_data')
    directories = [
        base_path / 'raw',
        base_path / 'processed', 
        base_path / 'partitioned',
        base_path / 'databases'
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    
    logging.info("Directories created")

def run_module(module_path, description, *args):
    logger = logging.getLogger(__name__)
    
    try:
        script_dir = os.path.dirname(module_path)
        original_cwd = os.getcwd()
        os.chdir(script_dir)
        
        spec = importlib.util.spec_from_file_location("module", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        original_argv = sys.argv.copy()
        sys.argv = list(args)
        
        module.main()
        
        sys.argv = original_argv
        os.chdir(original_cwd)
        logger.info(f"{description}")
        return True
        
    except Exception as e:
        logger.error(f"Error in {description}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        if 'original_cwd' in locals():
            os.chdir(original_cwd)
        return False

def run_complete_etl():
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting ETL...")
        ensure_directories()
        
        scripts = [
            ('/opt/airflow/scripts/01_fetch_data/fetch_vacancies.py', 'Fetching vacancies', 'fetch_vacancies.py', 'Data Engineer'),
            ('/opt/airflow/scripts/02_convert_to_csv/json_to_csv.py', 'Converting to CSV', 'json_to_csv.py'),
            ('/opt/airflow/scripts/03_sort_data/sorter.py', 'Sorting data', 'sorter.py'),
            ('/opt/airflow/scripts/04_change_data/cleaner.py', 'Enriching data', 'cleaner.py'),
            ('/opt/airflow/scripts/05_aggregate_data/counter.py', 'Aggregating data', 'counter.py'),
            ('/opt/airflow/scripts/06_partition_data/partitioner.py', 'Partitioning', 'partitioner.py'),
            ('/opt/airflow/scripts/06_partition_data/concatenator.py', 'Concatenating', 'concatenator.py')
        ]
        
        for module_path, description, *args in scripts:
            if not run_module(module_path, description, *args):
                raise Exception(f"Pipeline interrupted at: {description}")
        
        logger.info("ETL successfully completed!")
        return "SUCCESS"
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

with DAG(
    'hh_etl_final',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='run_complete_etl',
        python_callable=run_complete_etl
    )