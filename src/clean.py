import pandas as pd
import re
from typing import Tuple, Final

CLEAN_REGEX: Final = re.compile(r'\b(INC|LTD|CORP|CORPORATION|LIMITED|LLC|PLC|GMBH)\b\.?', re.IGNORECASE)
NON_ALPHANUM_REGEX: Final = re.compile(r'[^A-Z0-9\s]')
WHITESPACE_REGEX: Final = re.compile(r'\s+')

def clean_for_matching(series: pd.Series) -> pd.Series:
    return (series.astype(str).str.upper()
            .str.replace(CLEAN_REGEX, '', regex=True)
            .str.replace(NON_ALPHANUM_REGEX, ' ', regex=True)
            .str.replace(WHITESPACE_REGEX, ' ', regex=True).str.strip())

def normalize_series(series: pd.Series) -> pd.Series:
    return (series.astype(str).str.lower()
            .str.replace(WHITESPACE_REGEX, ' ', regex=True).str.strip())

def prepare_datasets(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    d1, d2 = df1.copy(), df2.copy()

    d1['addr_full'] = d1[['sStreet1', 'sCity', 'sProvState', 'sPostalZip']].fillna('').apply(lambda x: ', '.join(x.str.strip()).strip(', '), axis=1)
    d2['addr_full'] = d2[['address1', 'city', 'state', 'zip']].fillna('').apply(lambda x: ', '.join(x.str.strip()).strip(', '), axis=1)

    d1 = d1.rename(columns={'custname': 'name', 'sCity': 'city', 'sPostalZip': 'zip'})
    d2 = d2.rename(columns={'custname': 'name'})

    d1['street_compare'] = normalize_series(d1['sStreet1'].fillna(''))
    d2['street_compare'] = normalize_series(d2['address1'].fillna(''))

    for df in [d1, d2]:
        df['name_clean'] = clean_for_matching(df['name'])
        df['city'] = normalize_series(df['city'])
        df['zip'] = normalize_series(df['zip']).str.replace(r'\s+', '', regex=True)
            
    return d1, d2