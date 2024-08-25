# Celenium Vested Addresses

This project fetches addresses with vestings from the Celenium API and saves them to a CSV file.

## Setup

1. Ensure you have Python 3.8+ and pipenv installed.
2. Clone this repository.
3. Run `pipenv install` to set up the virtual environment and install dependencies.

## Usage

To run the script:

```
pipenv run python src/vested_addresses.py
```

The resulting CSV file will be saved in the `data/` directory.

## Note

This script respects API rate limits. It may take a significant amount of time to run if there are many addresses to process.