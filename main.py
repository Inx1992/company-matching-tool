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

    print("Step 2: Preparing location lists...")
    df1_temp = df1.copy()
    df1_temp['full_addr'] = df1_temp[['sStreet1', 'sCity', 'sProvState', 'sPostalZip']].fillna('').agg(', '.join, axis=1)
    d1_locations_map = df1_temp.groupby('custnmbr')['full_addr'].apply(lambda x: " | ".join(x.unique())).to_dict()

    df2_temp = df2.copy()
    df2_temp['full_addr'] = df2_temp[['address1', 'city', 'state', 'zip']].fillna('').agg(', '.join, axis=1)
    d2_locations_map = df2_temp.groupby('custnmbr')['full_addr'].apply(lambda x: " | ".join(x.unique())).to_dict()

    print("Step 3: Cleaning and normalization...")
    d1_clean, d2_clean = prepare_datasets(df1, df2)

    print("Step 4: Performing matching (Threshold: 82)...")
    match_results = find_best_matches(d1_clean, d2_clean, threshold=82)

    final_rows = []
    for res in match_results:
        row_to_save = df1.iloc[res['d1_index']].to_dict()
        cust_id_d1 = row_to_save['custnmbr']
        row_to_save['dataset1_locations_list'] = d1_locations_map.get(cust_id_d1, "")
        if res['d2_index'] is not None:
            d2_row = df2.iloc[res['d2_index']]
            cust_id_d2 = d2_row['custnmbr']
            row_to_save['match_name_d2'] = d2_row['custname']
            row_to_save['match_custnmbr_d2'] = cust_id_d2
            row_to_save['confidence_score'] = res['score']
            row_to_save['overlapping_locations'] = res['overlaps']
            row_to_save['dataset2_locations_list'] = d2_locations_map.get(cust_id_d2, "")
        else:
            row_to_save['match_name_d2'] = "NO MATCH"
            row_to_save['match_custnmbr_d2'] = None
            row_to_save['confidence_score'] = 0
            row_to_save['overlapping_locations'] = ""
            row_to_save['dataset2_locations_list'] = ""
        final_rows.append(row_to_save)

    output_df = pd.DataFrame(final_rows)
    matched_df = output_df[output_df['match_name_d2'] != "NO MATCH"]
    # match rate: % of Dataset 1 companies that have a match in Dataset 2
    total = len(df1)
    match_count = matched_df['custnmbr'].nunique()
    match_rate = (match_count / total) * 100
    # unmatched records: % of companies with no match in either dataset
    unmatched_perc = ((total - match_count) / total) * 100
    # one-to-many matches: % of companies with multiple matched entries
    one_to_many_count = (matched_df['custnmbr'].value_counts() > 1).sum()
    one_to_many_perc = (one_to_many_count / total) * 100

    print("\nFINAL METRICS:")
    print(f"Match Rate: {match_rate:.2f}%")
    print(f"Unmatched Records: {unmatched_perc:.2f}%")
    print(f"One-to-many Matches: {one_to_many_perc:.2f}%")

    os.makedirs('output', exist_ok=True)
    output_df.drop(columns=['match_custnmbr_d2']).to_csv('output/merged_companies.csv', index=False)
    print("\nDone! Result saved to output/merged_companies.csv")

if __name__ == "__main__":
    run_pipeline()