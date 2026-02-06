from rapidfuzz import fuzz, process

def find_best_matches(d1, d2, threshold=82):
    results = []
    d2_names = d2['name'].tolist()
    for i, row1 in d1.iterrows():
        best_match = process.extractOne(row1['name'], d2_names, scorer=fuzz.token_sort_ratio)
        d2_idx, score, overlaps = None, 0, []
        if best_match and best_match[1] >= threshold and len(str(row1['name'])) > 2:
            score, d2_idx = best_match[1], best_match[2]
            row2 = d2.iloc[d2_idx]
            if row1['city'] and row1['city'] == row2['city']: overlaps.append("city")
            if row1['zip'] and row1['zip'] == row2['zip']: overlaps.append("zip")
            s1, s2 = row1['street_full'], row2['street_full']
            if s1 and s2 and (s1 in s2 or s2 in s1): overlaps.append("street")
        results.append({
            'd1_index': i, 'd2_index': d2_idx, 
            'score': round(score, 2), 'overlaps': ", ".join(overlaps)
        })
    return results