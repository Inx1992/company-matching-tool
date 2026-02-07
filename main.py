import pandas as pd
import json, os
from src.clean import prepare_datasets
from src.match import find_best_matches

def run_pipeline():
    print("Step 1: Loading & Cleaning...")
    df1_raw = pd.read_csv('data/company_dataset_1.csv', dtype=str).fillna('')
    df2_raw = pd.read_csv('data/company_dataset_2.csv', dtype=str).fillna('')
    
    d1, d2 = prepare_datasets(df1_raw, df2_raw)
    
    d1_loc_map = d1.groupby('custnmbr')['addr_full'].apply(lambda x: " | ".join(filter(None, x.unique()))).to_dict()
    d2_loc_map = d2['addr_full'].to_dict()
    d2_name_map = d2['name'].to_dict()

    print("Step 2: Matching (Branch-aware logic)...")
    match_results = find_best_matches(d1, d2)
    res_df = pd.DataFrame(match_results)

    print("Step 3: Building Final Dataset & Metrics...")
    output = pd.DataFrame()
    output['custnmbr'] = d1['custnmbr']
    output['custname'] = d1['name']
    output['match_name_d2'] = res_df['d2_index'].map(d2_name_map).fillna("NO MATCH")
    output['dataset1_locations_list'] = d1['custnmbr'].map(d1_loc_map)
    output['dataset2_locations_list'] = res_df['d2_index'].map(d2_loc_map).fillna("")
    output['overlapping_locations'] = res_df['overlapping_locations']
    output['confidence_score'] = res_df['confidence_score']

    total_d1 = len(output)
    matched_df = output[output['match_name_d2'] != "NO MATCH"].copy()
    
    m_rate = (len(matched_df) / total_d1) * 100
    u_rate = 100 - m_rate
    
    match_counts = matched_df['match_name_d2'].value_counts()
    otm_count = (match_counts > 1).sum()
    otm_perc = (otm_count / total_d1) * 100

    perfect = len(matched_df[matched_df['confidence_score'] == 100])
    high = len(matched_df[(matched_df['confidence_score'] >= 90) & (matched_df['confidence_score'] < 100)])
    borderline = len(matched_df[(matched_df['confidence_score'] >= 60) & (matched_df['confidence_score'] < 90)])

    exact_names = (perfect / len(matched_df)) * 100 if not matched_df.empty else 0
    fuzzy_names = 100 - exact_names

    def count_locs(s): return len(s.split(',')) if s else 0
    loc_counts = matched_df['overlapping_locations'].apply(count_locs)
    
    full_loc = (len(matched_df[loc_counts >= 3]) / len(matched_df)) * 100 if not matched_df.empty else 0
    partial_loc = (len(matched_df[(loc_counts > 0) & (loc_counts < 3)]) / len(matched_df)) * 100 if not matched_df.empty else 0
    name_only = (len(matched_df[loc_counts == 0]) / len(matched_df)) * 100 if not matched_df.empty else 0

    print(f"\n" + "="*50)
    print(f"{'FINAL MATCHING QUALITY REPORT':^50}")
    print("="*50)
    print(f"Match Rate:          {m_rate:.2f}%")
    print(f"Unmatched Records:   {u_rate:.2f}%")
    print(f"One-to-many matches: {otm_perc:.2f}% ({otm_count} branch clusters)")
    
    print(f"\nScore Distribution:")
    print(f" - Perfect (100%):   {perfect} matches")
    print(f" - High (90-99%):    {high} matches")
    print(f" - Borderline (60%): {borderline} matches")
    
    print(f"\nMatch Quality:")
    print(f" - Exact Name Matches: {exact_names:.2f}%")
    print(f" - Fuzzy Name Matches: {fuzzy_names:.2f}%")
    
    print(f"\nLocation Verification Strength:")
    print(f" - Full (City+ZIP+Street): {full_loc:.2f}%")
    print(f" - Partial:                {partial_loc:.2f}%")
    print(f" - Name only (No overlap): {name_only:.2f}%")
    print("="*50)

    metrics_data = {
        "overall": {
            "match_rate": round(m_rate, 2),
            "unmatched_rate": round(u_rate, 2),
            "one_to_many_cases": int(otm_count),
            "one_to_many_percentage": round(otm_perc, 2)
        },
        "score_distribution": {
            "perfect": perfect,
            "high": high,
            "borderline": borderline
        },
        "match_quality": {
            "exact_names_percent": round(exact_names, 2),
            "fuzzy_names_percent": round(fuzzy_names, 2)
        },
        "location_verification": {
            "full_overlap_percent": round(full_loc, 2),
            "partial_overlap_percent": round(partial_loc, 2),
            "name_only_percent": round(name_only, 2)
        }
    }

    os.makedirs('output', exist_ok=True)
    
    output.to_csv('output/merged_companies.csv', index=False)
    
    with open('output/matching_metrics.json', 'w', encoding='utf-8') as f:
        json.dump(metrics_data, f, indent=4, ensure_ascii=False)

    print(f"\nDone! Results saved to:")
    print(f" - CSV:  output/merged_companies.csv")
    print(f" - JSON: output/matching_metrics.json")

if __name__ == "__main__":
    run_pipeline()