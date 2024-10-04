# Sirene Loader

The **Sirene Loader** is a Python application designed to fetch data from an API, process it using Apache Spark, and manage it within Delta Lake tables. This project utilizes various configurations for flexibility and scalability, making it suitable for data ingestion workflows.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)

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

You can install the required packages using pip:

```bash
git clone https://github.com/Nouuh/sirene_loader.git
cd sirene_loader
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt