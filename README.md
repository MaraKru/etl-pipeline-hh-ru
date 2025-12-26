# ETL Pipeline for HH.ru Job Vacancies

This project is a production-style ETL pipeline for collecting and analyzing job vacancy data from HH.ru API, orchestrated with Apache Airflow and containerized using Docker.


### About HH.ru

HH.ru is Russia's largest job search and recruitment platform, analogous to LinkedIn.


## Project Goal

Create a program for collecting job vacancy data, processing it, and preparing it for analytics, while learning Docker and Apache Airflow in practice.


### Technology Stack

- **Python** - development language (3.13 for manual runs, 3.11 in Docker)
- **Apache Airflow** - workflow orchestration
- **Docker** - containerization
- **PostgreSQL** - database for Airflow
- **SQLite** - database for ETL results
- **Requests** - API interactions
- **CSV/JSON** - data storage formats
- **Logging** - pipeline execution logging
- **Pathlib** - cross-platform path handling


### ETL Pipeline consists of 7 stages:

1. **Extract** - fetch data via HH.ru API
2. **Convert** - transform JSON to CSV
3. **Sort** - sort vacancies
4. **Enrich** - data enrichment (determine position level)
5. **Aggregate** - aggregation and statistics + save to SQLite
6. **Partition** - date-based partitioning
7. **Concatenate** - file merging


## Getting Started

### Local ETL (docker-compose.yml)

```bash
docker-compose run --rm etl python src/run_etl.py "Data Engineer"
# docker-compose run --rm etl python src/run_etl.py "Data Scientist"
# docker-compose run --rm etl python src/run_etl.py "Data Analyst"
```


### Local ETL with Database Viewing

```bash
docker-compose run --rm etl bash -c "python src/run_etl.py 'Data Engineer' && python view.py"
# docker-compose run --rm etl bash -c "python src/run_etl.py 'Data Scientist' && python view.py"
# docker-compose run --rm etl bash -c "python src/run_etl.py 'Data Analyst' && python view.py"
```


### Production with Airflow (docker-compose-airflow.yml)

```bash
docker-compose -f docker-compose-airflow.yml up -d    # start
docker-compose -f docker-compose-airflow.yml logs -f   # logs
docker-compose -f docker-compose-airflow.yml down     # stop
```

### Manual Run (without Docker)

```bash
pip install requests pandas
python src/run_etl.py "Data Engineer"
python view.py
```


## Troubleshooting

### Permission Issues (Windows):

The project automatically uses container-native storage (/tmp) to bypass Docker volume limitations on Windows.


**If permission errors still occur:**

```bash
# Grant permissions to data folder (Linux/Mac)
chmod -R 755 data
```

### Missing Modules

All paths are managed via scripts/config.py


## Project Evolution

The project started as a learning assignment but was significantly enhanced.


### Architecture Refactoring

- **Before**: Fragmented bash scripts
- **After**: Unified Python pipeline with cross-platform compatibility and Docker containerization


### Improvements

- Containerization with Docker
- Orchestration with Apache Airflow
- PostgreSQL for Airflow
- SQLite database for results
- File and console logging
- Error handling at each stage
- Clear folder structure


### Challenges and Engineering Insights

This project presented *more* operational system challenges than any other I've worked on. While I knew Windows had limitations, navigating them in this project proved really difficult. Beyond independently learning Apache Airflow and setting up PostgreSQL, I had to rewrite docker-compose multiple times. This was a valuable hands-on experience and my first deep dive into Docker.
Despite these hurdles, the project is complete, and I find myself considering a switch to Linux. It's become clear that the choice of operating system significantly impacts development workflows and capabilities.