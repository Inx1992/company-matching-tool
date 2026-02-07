from rapidfuzz import fuzz, process
import pandas as pd
from typing import List, Dict, Any

def find_best_matches(d1: pd.DataFrame, d2: pd.DataFrame, threshold: int = 60) -> List[Dict[str, Any]]:
    d2_names = d2['name_clean'].tolist()
    d2_indices = d2.index.tolist()
    d2_data = d2[['city', 'zip', 'street_compare']].to_dict('index')
    results = []
    
    for i, row1 in d1.iterrows():
        name1 = row1['name_clean']
        if not name1:
            results.append({'d1_index': i, 'd2_index': None, 'confidence_score': 0.0, 'overlapping_locations': ""})
            continue

        best_match = process.extractOne(name1, d2_names, scorer=fuzz.token_sort_ratio)
        d2_idx, score, overlaps = None, 0.0, []
        
        if best_match and best_match[1] >= threshold:
            current_score = best_match[1]
            current_d2_idx = d2_indices[best_match[2]]
            row2 = d2_data[current_d2_idx]
            
            city_match = row1['city'] and row1['city'] == row2['city']
            zip_match = row1['zip'] and row1['zip'] == row2['zip']
            street_match = row1['street_compare'] and (row1['street_compare'] in row2['street_compare'] or row2['street_compare'] in row1['street_compare'])
            
            if city_match: overlaps.append("city")
            if zip_match: overlaps.append("zip")
            if street_match: overlaps.append("street")
            
            if current_score >= 95:
                d2_idx, score = current_d2_idx, current_score
            elif current_score >= 85 and overlaps:
                d2_idx, score = current_d2_idx, current_score
            elif current_score >= threshold:
                if street_match or (city_match and zip_match):
                    d2_idx, score = current_d2_idx, current_score
        
        results.append({
            'd1_index': i,
            'd2_index': d2_idx,
            'confidence_score': round(score, 2),
            'overlapping_locations': ", ".join(overlaps)
        })
    return results