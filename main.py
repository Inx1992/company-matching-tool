import pandas as pd
import os
from src.clean import prepare_datasets
from src.match import find_best_matches

def run_pipeline():
    print("Step 1: Loading datasets...")
    try:
        df1 = pd.read_csv('data/company_dataset_1.csv', dtype=str)
        df2 = pd.read_csv('data/company_dataset_2.csv', dtype=str)
    except FileNotFoundError:
        print("Error: Files not found in data/ directory")
        return

    print("Step 2: Cleaning and normalization...")
    d1_clean, d2_clean = prepare_datasets(df1, df2)

    print("Step 3: Performing matching (Threshold: 82)...")
    match_results = find_best_matches(d1_clean, d2_clean, threshold=82)

    final_rows = []
    for res in match_results:
        row_to_save = df1.iloc[res['d1_index']].to_dict()
        
        if res['d2_index'] is not None:
            d2_row = df2.iloc[res['d2_index']]
            row_to_save['match_name_d2'] = d2_row['custname']
            row_to_save['match_custnmbr_d2'] = d2_row['custnmbr']
            row_to_save['confidence_score'] = res['score']
            row_to_save['overlapping_locations'] = res['overlaps']
        else:
            row_to_save['match_name_d2'] = "NO MATCH"
            row_to_save['match_custnmbr_d2'] = None
            row_to_save['confidence_score'] = 0
            row_to_save['overlapping_locations'] = ""
            
        final_rows.append(row_to_save)

    output_df = pd.DataFrame(final_rows)
    matched_df = output_df[output_df['match_name_d2'] != "NO MATCH"]
    
    total = len(df1)
    match_count = len(matched_df)
    one_to_many_count = matched_df['match_custnmbr_d2'].duplicated().sum() if match_count > 0 else 0

    print("\nFINAL METRICS:")
    print(f"Match Rate: {(match_count/total)*100:.2f}%")
    print(f"Unmatched: {((total - match_count)/total)*100:.2f}%")
    print(f"One-to-many matches: {(one_to_many_count/total)*100:.2f}% ({one_to_many_count} cases)")

    if match_count > 0:
        print(f"Avg Confidence Score: {matched_df['confidence_score'].mean():.2f}%")
        
        # 1. Distribution of Confidence Scores
        perfect = (matched_df['confidence_score'] == 100).sum()
        high = ((matched_df['confidence_score'] >= 90) & (matched_df['confidence_score'] < 100)).sum()
        borderline = ((matched_df['confidence_score'] >= 82) & (matched_df['confidence_score'] < 90)).sum()
        
        print(f"\nScore Distribution:")
        print(f"   - Perfect (100%): {perfect} matches")
        print(f"   - High (90-99%): {high} matches")
        print(f"   - Borderline (82-89%): {borderline} matches")
        
        # 2. Match Quality (Exact vs Fuzzy)
        print(f"\nMatch Quality:")
        print(f"   - Exact Name Matches: {(perfect/match_count)*100:.2f}%")
        print(f"   - Fuzzy Name Matches: {((high + borderline)/match_count)*100:.2f}%")
        
        # 3. Strength of Location Verification
        def count_overlaps(x):
            return len([i for i in str(x).split(', ') if i])
            
        loc_counts = matched_df['overlapping_locations'].apply(count_overlaps)
        full_verify = (loc_counts >= 3).sum()
        partial_verify = ((loc_counts > 0) & (loc_counts < 3)).sum()
        no_verify = (loc_counts == 0).sum()
        
        print(f"\nLocation Verification Strength:")
        print(f"   - Full (City+ZIP+Street): {(full_verify/match_count)*100:.2f}%")
        print(f"   - Partial: {(partial_verify/match_count)*100:.2f}%")
        print(f"   - Name only (No overlap): {(no_verify/match_count)*100:.2f}%")

    os.makedirs('output', exist_ok=True)
    output_df.drop(columns=['match_custnmbr_d2']).to_csv('output/merged_companies.csv', index=False)
    
    print("\nDone! Result saved to output/merged_companies.csv")

if __name__ == "__main__":
    run_pipeline()