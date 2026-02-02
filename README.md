# Company Data Matching Tool

A robust Python-based solution for merging and matching company records across disparate datasets. This tool uses fuzzy string matching logic combined with geographic verification to ensure high data reliability.

## Project Structure

- `main.py`: The entry point that orchestrates the data pipeline and reports metrics.
- `src/clean.py`: Contains logic for data normalization, address consolidation, and text cleaning.
- `src/match.py`: Implements fuzzy matching using `rapidfuzz` and geographic overlap verification.
- `data/`: Directory for input CSV datasets.
- `output/`: Directory where the final `merged_companies.csv` is saved.

## Key Features

- **Fuzzy Matching**: Uses `token_sort_ratio` to handle name variations, acronyms, and word order differences.
- **Location Verification**: Cross-references City, ZIP code, and Street address to validate matches.
- **Advanced Analytics**: Provides a detailed breakdown of match quality (Perfect vs. Fuzzy) and verification strength.

## Installation

Ensure you have Python 3.8+ installed. Install the required dependencies:

```bash
pip install -r requirements.txt
```
