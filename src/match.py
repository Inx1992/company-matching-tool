from rapidfuzz import fuzz, process

def find_best_matches(d1, d2, threshold=82):
    results = []
    d2_names = d2['name'].tolist()

    for i, row1 in d1.iterrows():
        match_info = process.extractOne(row1['name'], d2_names, scorer=fuzz.token_sort_ratio)
        
        best_row2, score, idx = None, 0, -1
        
        if match_info:
            target_name, score, idx = match_info
            if score >= threshold and len(row1['name']) > 2:
                best_row2 = d2.iloc[idx]
        
        overlaps = []
        if best_row2 is not None:
            if row1['city'] == best_row2['city'] and row1['city'] != "":
                overlaps.append("city")
            
            if row1['zip'] == best_row2['zip'] and row1['zip'] != "":
                overlaps.append("zip")
            
            s1, s2 = row1['street_full'], best_row2['street_full']
            if s1 != "" and s2 != "" and (s1 in s2 or s2 in s1):
                overlaps.append("street")

            results.append({
                'd1_index': i,
                'd2_index': idx,
                'score': round(score, 2),
                'overlaps': ", ".join(overlaps)
            })
        else:
            results.append({
                'd1_index': i,
                'd2_index': None,
                'score': 0,
                'overlaps': ""
            })
            
    return results