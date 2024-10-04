# Sirene Loader

**Sirene Loader** is a Python application designed to fetch economic data from the Opendatasoft API, process it using Apache Spark, and manage data storage with Delta Lake. This project facilitates the initialization and incremental updates of a Delta table, driven by configurations defined in a YAML file.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)

## Features

- Fetches data from an external API and saves it to a specified location.
- Supports both initializing and updating Delta Lake tables.
- Configurable via a YAML file for flexibility.
- Uses Apache Spark for data processing.
- Simple command-line interface for running the application.

## Requirements

- Python 
- Apache Spark
- Delta Lake
- PyYAML
- Requests


## Installation

```bash
git clone https://github.com/Nouuh/sirene_loader.git #Clone the repository**:
cd sirene_loader
python -m venv venv #Create a virtual environment (optional but recommended):
source venv/bin/activate
pip install -r requirements.txt #Install dependencies
```

## Usage

To execute the application, you must specify the run type and, a date. The provided date will determine the range of data fetched from OpenDataSoft, starting from the specified date up to the most recent available data. Use the command below:

```bash
python main.py --run_type <run_type> --date <YYYY-MM-DD>
```

Example: 

```bash
python main.py --run_type init --date 2024-10-04
```