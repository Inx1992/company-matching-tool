# Company Data Matching Tool

A robust Python-based solution for merging and matching company records across disparate datasets. This tool uses advanced fuzzy string matching logic combined with multi-layer geographic verification (Address, ZIP, City) to ensure high data reliability.

## 📁 Project Structure

- `main.py`: Entry point orchestrating the pipeline, normalization, and metrics reporting.
- `src/clean.py`: Logic for structural data preparation and column mapping.
- `src/match.py`: Implementation of fuzzy matching using `rapidfuzz` and geographic overlap logic.
- `data/`: Source CSV files (`company_dataset_1.csv` and `company_dataset_2.csv`).
- `output/`: Results including `merged_companies.csv` and `metrics.json`.

## 🛠 Matching Approach

The tool employs a multi-stage approach to Entity Resolution:

1. **Structural Normalization**: Consolidation of multi-line addresses (Street 1-3) into a single string.
2. **Deep Text Cleaning**: Removal of legal suffixes (Inc, Ltd, Corp, LLC) and special characters using Regex to isolate the "core" company name.
3. **Fuzzy Scoring**: Utilizing `token_sort_ratio` which is resilient to word reordering (e.g., "Plumbing Division 25" vs "Division 25 Plumbing").
4. **Geographic Validation**: A secondary verification layer that checks for overlaps in City, ZIP, and Street names.

## 🔍 Data Quality Issues Found

During analysis of the source datasets, several critical issues were identified and handled:

- **Fixed-width Padding**: Dataset 2 contained company names with significant trailing spaces.
- **Suffix Variations**: Inconsistent legal entity descriptors (e.g., "Limited" vs "Ltd").
- **Postal Code Inconsistency**: Differences in spacing (e.g., "M6L 3C1" vs "M6L3C1").
- **Address Fragmentation**: Primary location data split across different numbers of columns (2 in D1, 3 in D2).

## ⚙️ Transformations Applied

- **Regex Normalization**: All names converted to uppercase, stripped of legal entities, and cleared of non-alphanumeric noise.
- **ZIP Stripping**: Removal of all whitespace from postal codes to ensure a 1:1 match.
- **Address Aggregation**: Smart joining of address fields with null-handling to prevent "hanging commas".
- **Fuzzy Thresholding**: Optimized threshold of **82** to catch variations like "TX Mechanical" vs "VTN Mechanical".

## 📊 Calculated Metrics

The tool calculates and exports the following KPIs to `output/metrics.json`:

- **Match Rate**: % of Dataset 1 companies successfully linked to Dataset 2.
- **Unmatched Records**: % of companies remaining unique to Dataset 1.
- **One-to-Many Matches**: % of records with multiple potential entries (duplicates).
- **Average Confidence Score**: Overall reliability of the matched pairs.

## 🚀 Installation & Usage

1. Ensure you have Python 3.8+ installed.
2. Install dependencies:
   ```bash
   pip install pandas rapidfuzz
   Run the tool:
   ```

Bash
python main.py

## ⚙️ Threshold Configuration (Tuning)

The sensitivity is controlled by the threshold parameter in main.py:

```python
match_results = find_best_matches(d1_clean, d2_clean, threshold=82)
```

- **90-100 (Strict)**: Low risk of errors, but might miss companies with different legal forms.

- **82 (Balanced)**: Current default. Optimal balance for handling typos and suffix differences.

- **70-80 (Loose)**: Catches more variations but requires manual audit of overlapping_locations.

## 📝 Technical Notes

A match is considered Highly Reliable if:

The confidence_score is > 85.

OR the overlapping_locations contains at least two geographic matches (e.g., "city, zip").
