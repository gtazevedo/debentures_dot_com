import requests
import io
import pandas as pd
import locale
from dateutil import parser


def get_response_to_pd(url: str) -> pd.DataFrame:
    try:
        response = requests.get(url, timeout=10)         
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        df = pd.read_csv(io.StringIO(response.text), sep='|', encoding='utf-8', names=['raw'], skiprows=2)
        df = df['raw'].str.split('\t', expand=True).reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)[:-2]
        return df
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error for {url}: {e}")
        return pd.DataFrame()
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error for {url}: {e}")
        return pd.DataFrame()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error for {url}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred for {url}: {e}")
        return pd.DataFrame()
    
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

def _format_date_for_url(date_input: str, dtfmt:str = None, locale_:str = None) -> str:
    """
    Converts a date string (in various formats) to DD%2FMM%2FYYYY for URL usage.
    Returns an empty string if the date cannot be parsed.
    """
    if locale_ is None:
        locale_ = 'pt_BR.utf8'
    locale.setlocale(locale.LC_TIME, locale_)
    if dtfmt is None:
        dtfmt = '%d%%2F%m%%2F%Y'
    if not isinstance(date_input, str) or not date_input:
        return ''
    try:
        parsed_date = parser.parse(date_input)
        return parsed_date.strftime(dtfmt)
    except ValueError:
        return '' # Invalid date format