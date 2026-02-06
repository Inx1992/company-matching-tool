import pandas as pd
import os, json
from src.clean import prepare_datasets
from src.match import find_best_matches

def run_pipeline():
    try:
        print("Step 1: Loading datasets...")
        df1 = pd.read_csv('data/company_dataset_1.csv', dtype=str).fillna('')
        df2 = pd.read_csv('data/company_dataset_2.csv', dtype=str).fillna('')
    except FileNotFoundError: 
        return print("Error: Files not found. Ensure CSV files are in /data folder.")

    print("Step 2: Preparing location lists and cleaning strings...")
    df1_temp, df2_temp = df1.copy(), df2.copy()
    df1_temp['addr'] = df1_temp[['sStreet1', 'sCity', 'sProvState', 'sPostalZip']].agg(', '.join, axis=1).str.strip(', ')
    d1_map = df1_temp.groupby('custnmbr')['addr'].apply(lambda x: " | ".join(filter(None, x.unique()))).to_dict()
    df2_temp['addr'] = df2_temp[['address1', 'city', 'state', 'zip']].agg(', '.join, axis=1).str.strip(', ')
    d2_map = df2_temp.groupby('custnmbr')['addr'].apply(lambda x: " | ".join(filter(None, x.unique()))).to_dict()

    print("Step 3: Cleaning and normalization...")
    d1_clean, d2_clean = prepare_datasets(df1, df2)

    print("Step 4: Performing matching (Threshold: 82)...")
    results = find_best_matches(d1_clean, d2_clean, threshold=82)
    
    final_rows = []
    for res in results:
        row = df1.iloc[res['d1_index']].to_dict()
        cid1 = row['custnmbr']
        row.update({
            'dataset1_locations_list': d1_map.get(cid1, ""), 
            'match_name_d2': "NO MATCH", 'confidence_score': 0, 
            'overlapping_locations': "", 'dataset2_locations_list': ""
        })
        if res['d2_index'] is not None:
            d2_row = df2.iloc[res['d2_index']]
            row.update({
                'match_name_d2': str(d2_row['custname']).strip(), 
                'confidence_score': res['score'], 
                'overlapping_locations': res['overlaps'], 
                'dataset2_locations_list': d2_map.get(d2_row['custnmbr'], "")
            })
        final_rows.append(row)

    output_df = pd.DataFrame(final_rows)
    matched = output_df[output_df['match_name_d2'] != "NO MATCH"]
    total = len(df1)
    
    m_count = matched['custnmbr'].nunique()
    m_rate = (m_count / total) * 100
    u_rate = 100 - m_rate
    otm_count = (matched['custnmbr'].value_counts() > 1).sum()
    otm_perc = (otm_count / total) * 100
    avg_conf = matched['confidence_score'].mean() if not matched.empty else 0

    perf_c = len(matched[matched['confidence_score'] == 100])
    high_c = len(matched[(matched['confidence_score'] >= 90) & (matched['confidence_score'] < 100)])
    bord_c = len(matched[(matched['confidence_score'] >= 82) & (matched['confidence_score'] < 90)])

    v_counts = matched['overlapping_locations'].apply(lambda x: len(x.split(', ')) if x else 0)
    f_v_p = (v_counts == 3).sum() / len(matched) * 100 if len(matched) > 0 else 0
    p_v_p = (v_counts.isin([1, 2])).sum() / len(matched) * 100 if len(matched) > 0 else 0
    n_v_p = (v_counts == 0).sum() / len(matched) * 100 if len(matched) > 0 else 0

    metrics = {
        "summary": {"match_rate": round(m_rate, 2), "unmatched": round(u_rate, 2), "otm_cases": int(otm_count), "avg_score": round(avg_conf, 2)},
        "distribution": {"perfect": perf_c, "high": high_c, "borderline": bord_c},
        "quality": {"exact_p": round(perf_c/len(matched)*100, 2), "fuzzy_p": round((len(matched)-perf_c)/len(matched)*100, 2)},
        "verification": {"full_p": round(f_v_p, 2), "partial_p": round(p_v_p, 2), "none_p": round(n_v_p, 2)}
    }

    print(f"\nFINAL METRICS:")
    print(f"Match Rate: {m_rate:.2f}%")
    print(f"Unmatched: {u_rate:.2f}%")
    print(f"One-to-many matches: {otm_perc:.2f}% ({otm_count} cases)")
    print(f"Avg Confidence Score: {avg_conf:.2f}%")
    print(f"\nScore Distribution:")
    print(f"   - Perfect (100%): {perf_c} matches")
    print(f"   - High (90-99%): {high_c} matches")
    print(f"   - Borderline (82-89%): {bord_c} matches")
    print(f"\nMatch Quality:")
    print(f"   - Exact Name Matches: {metrics['quality']['exact_p']}%")
    print(f"   - Fuzzy Name Matches: {metrics['quality']['fuzzy_p']}%")
    print(f"\nLocation Verification Strength:")
    print(f"   - Full (City+ZIP+Street): {metrics['verification']['full_p']}%")
    print(f"   - Partial: {metrics['verification']['partial_p']}%")
    print(f"   - Name only (No overlap): {metrics['verification']['none_p']}%")

    os.makedirs('output', exist_ok=True)
    with open('output/metrics.json', 'w', encoding='utf-8') as f: json.dump(metrics, f, indent=4)
    output_df.to_csv('output/merged_companies.csv', index=False)
    
    print(f"\n Done!")
    print(f" Metrics saved to: output/metrics.json")
    print(f" Results saved to: output/merged_companies.csv")

if __name__ == "__main__":
    run_pipeline()