import requests
import io
import pandas as pd
from dateutil import parser

def get_response_to_pd(url:str)->pd.DataFrame:
    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text), sep='|', encoding='utf-8', names=['raw'], skiprows=2)
    df = df['raw'].str.split('\t', expand=True).reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)[:-2]
    return df

def _format_cnpj(cnpj: str) -> str:
    """
    Formats a CNPJ string to be 14 digits, padding with leading zeros if necessary.
    Returns an empty string if the input is not a valid CNPJ (e.g., non-numeric or too long).
    """
    if not isinstance(cnpj, str):
        return ''
    
    cnpj_digits = ''.join(filter(str.isdigit, cnpj))
    
    if len(cnpj_digits) > 14:
        return '' # CNPJ too long
    
    return cnpj_digits.zfill(14)

def _format_date_for_url(date_input: str) -> str:
    """
    Converts a date string (in various formats) to DD%2FMM%2FYYYY for URL usage.
    Returns an empty string if the date cannot be parsed.
    """
    if not isinstance(date_input, str) or not date_input:
        return ''
    try:
        parsed_date = parser.parse(date_input)
        return parsed_date.strftime('%d%%2F%m%%2F%Y')
    except ValueError:
        return '' # Invalid date format