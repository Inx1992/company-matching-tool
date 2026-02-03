# Company Data Matching Tool

A robust Python-based solution for merging and matching company records across disparate datasets. This tool uses fuzzy string matching logic combined with geographic verification (Address, ZIP, City) to ensure high data reliability.

## Project Structure

- `main.py`: The entry point that orchestrates the data pipeline and reports metrics.
- `src/clean.py`: Contains logic for data normalization, address consolidation, and text cleaning.
- `src/match.py`: Implements fuzzy matching using `rapidfuzz` and geographic overlap verification.
- `data/`: Directory for input CSV datasets (Dataset 1 and Dataset 2).
- `output/`: Directory where the final `merged_companies.csv` is saved.

## Key Features

- **Fuzzy Matching**: Uses `token_sort_ratio` to handle name variations, acronyms, and word order differences.
- **Location Verification**: Cross-references City, ZIP code, and Street address to validate matches and reduce false positives.
- **Advanced Analytics**: Provides a detailed breakdown of match quality and verification strength.

## Installation & Usage

1. Ensure you have Python 3.8+ installed.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## ⚙️ Threshold Configuration (Tuning)

The sensitivity of the matching algorithm is controlled by the `threshold` parameter in `main.py`.

**Current Setting:** `threshold=82` (Optimal balance).

To adjust the matching sensitivity, find the run_pipeline() function in main.py and modify the threshold value (currently set to 82):

```python
match_results = find_best_matches(d1_clean, d2_clean, threshold=82)
```

- Higher Threshold (90-100): Results in Strict matching. Low risk of errors, but might miss companies with different legal forms.

- Lower Threshold (70-80): Results in Loose matching. Catches more variations but requires manual verification

## Results and Performance

After processing the provided datasets, the tool generates the following metrics:

- Total Matches Found: ~51% of records from Dataset 2 were linked.

- Accuracy: ~80% of matches are Perfect Matches (100% score).

- Verification Level: Most fuzzy matches were confirmed by at least two geographic attributes (City + ZIP).

## Technical Notes

The tool uses a Confidence Score to rank matches. A match is considered highly reliable if the Confidence_Score is above 85 or if the geographic verification returns True for multiple fields.
