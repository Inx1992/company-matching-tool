# Company Data Matching Tool

A professional Python-based solution for entity resolution and branch-aware matching across disparate datasets.

## 📁 Project Structure

- `main.py`: Entry point orchestrating the pipeline and generating JSON analytics.
- `src/clean.py`: Advanced normalization (Regex-based name cleaning, ZIP standardization).
- `src/match.py`: **Core Engine** featuring tiered validation logic (Fuzzy Matching + Geo-Verification).
- `data/`: Source CSV files (`company_dataset_1.csv` and `company_dataset_2.csv`).
- `output/`: High-fidelity results (`merged_companies.csv`) and technical KPIs (`matching_metrics.json`).

## 🛠 Advanced Matching Strategy (Tiered Logic)

Unlike standard fuzzy matching, this tool uses a **conditional verification pipeline** to eliminate False Positives:

1.  **Tier 1: Branch/Corporate Match (Score 95-100)**
    - High string similarity allows for matching even across different cities (identifying corporate branches).
2.  **Tier 2: High Confidence Match (Score 85-94)**
    - Requires at least one geographic overlap (City, ZIP, or Street) to confirm the match.
3.  **Tier 3: Low Similarity Match (Score 60-84)**
    - Strict requirement: Match is only accepted if the **Street** is identical OR both **City and ZIP** match perfectly. This "pulls in" valid records with significant typos.

## 🔍 Data Quality Issues Handled

- **Trailing Padding**: Handled fixed-width string noise from Dataset 2.
- **Branch Specificity**: Identified location-specific suffixes in brackets (e.g., "Company Inc. (Toronto Branch)").
- **Address Fragmentation**: Aggregated multiple address lines into a searchable "street_compare" field.
- **Postal Code Noise**: Normalized "M1M 1M1" vs "M1M1M1" for 1:1 comparison.

## 📊 Performance Metrics (Current Run)

The latest execution achieved a high-quality linkage with the following breakdown:

- **Match Rate**: **52.57%** (Optimized via Tier 3 logic).
- **Branch Clusters**: **47 groups** identified (One-to-Many matches).
- **Location Strength**: **~60%** of matches are "Full Overlap" (City + ZIP + Street).
- **Confidence**: **85%** of matches are Exact Name matches.

## 🚀 Installation & Usage

1. Install dependencies:
   ```bash
   pip install pandas rapidfuzz
   ```
2. Run the pipeline:

```Bash
python main.py
```

## ⚙️ Threshold Configuration (Tuning)

The sensitivity is controlled by the `threshold` parameter in `main.py`. Based on the multi-layer logic in `src/match.py`, the behavior is as follows:

- **90-100 (Strict)**: Extremely low risk of errors. Ideal for clean datasets where legal forms are consistent.
- **82-89 (Balanced)**: Good for handling minor typos and suffix differences. Requires at least one geographic overlap.
- **60-81 (Verified Loose)**: Current setting. Catches significant variations (e.g., "TX Mechanical" vs "VTN Mechanical") but **enforces mandatory street/postal verification** to maintain precision.

```python
# Sensitivity control in main.py
match_results = find_best_matches(d1, d2, threshold=60)
```

## 📝 Technical Notes

- **A match is considered Highly Reliable if**:

* The confidence_score is > 85 (High name similarity).

* OR the overlapping_locations contains at least two geographic matches (e.g., "city, zip") or a confirmed "street".
