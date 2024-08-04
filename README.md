# MongoDB to BigQuery Data Pipeline

This is a data pipeline that copies data from MongoDB to Google BigQuery using Apache Beam. It reads data from specified MongoDB collections and writes it to corresponding BigQuery tables. It's designed to handle multiple hosts, databases and collections.

## Configuration

The pipeline uses a CSV file to specify the source and destination details. The CSV file should have the following structure:

- `host_name`: The MongoDB host
- `database_name`: The source MongoDB database
- `collection_name`: The source MongoDB collection
- `table_name`: The destination BigQuery table

## Requirements

- Python 3.7+
- Poetry (for dependency management)
- Google Cloud SDK

## Setup

1. Clone this repository
2. Install Poetry if you haven't already: `pip install poetry`
3. Change directory using `cd mongo_to_biquery_beam`
4. Activate virtual environment `poetry shell`
5. install dependecies `poetry install`
6. Run `python3 etl.py` or `python etl.py` depending on your setup

Note: You can also pass command line arguments for the parameters defined in the .env file. For example: `python etl.py --dataset=your_dataset`

