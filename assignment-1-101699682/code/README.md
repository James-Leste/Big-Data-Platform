# Source code structure

- `cassandra-compose.yml`: contains cassandra cluster docker image configuration.
- `connector.py`: contains `createKeyspace()`, `initDatabase()` and `initinitConnection()` functions for database connection.
- `mysimbdp-dataingest.py`: contains `ingesting()` for data ingestion.
- `requirements.txt`: contains python package requirements.
