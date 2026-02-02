import pandas as pd

def normalize_text(text):
    if pd.isna(text): 
        return ""
    return " ".join(str(text).split()).lower().strip()

def prepare_datasets(df1, df2):
    d1 = df1.copy()
    d1['street_full'] = d1[['sStreet1', 'sStreet2']].fillna('').agg(' '.join, axis=1)
    d1 = d1.rename(columns={
        'custname': 'name', 
        'sCity': 'city', 
        'sProvState': 'state', 
        'sPostalZip': 'zip'
    })
    
    d2 = df2.copy()
    d2['street_full'] = d2[['address1', 'address2', 'address3']].fillna('').agg(' '.join, axis=1)
    d2 = d2.rename(columns={
        'custname': 'name', 
        'city': 'city', 
        'state': 'state', 
        'zip': 'zip'
    })

    for df in [d1, d2]:
        cols_to_fix = ['name', 'street_full', 'city', 'zip']
        for col in cols_to_fix:
            if col in df.columns:
                df[col] = df[col].apply(normalize_text)
        
        if 'zip' in df.columns:
            df['zip'] = df['zip'].str.replace(r'\s+', '', regex=True)
            
    return d1, d2