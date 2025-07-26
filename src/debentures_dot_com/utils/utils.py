import requests
import io
import pandas as pd
import locale
from bs4 import BeautifulSoup
from dateutil import parser


def get_response_to_pd(url: str, sep:str = None, headers:dict=None, data:dict=None,timeout:int=None,skiprows:int=None) -> pd.DataFrame:
    if sep is None:
        sep = '|'
    if timeout is None:
        timeout= 10
    if skiprows is None:
        skiprows = 2
    try:
        response = requests.get(url, headers=headers, data=data, timeout=timeout)         
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        df = pd.read_csv(io.StringIO(response.text), sep=sep, encoding='utf-8', names=['raw'], skiprows=skiprows)
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
    
def simple_response_to_pd(url:str, sep:str, encoding:str=None, names:list=None, skiprows:int=None) ->pd.DataFrame:
    encoding = encoding if isinstance(encoding, str) else 'utf-8'
    names = names if isinstance(names, list) else ['raw']
    skiprows = skiprows if isinstance(skiprows, int) else 2
    try:
        response = requests.get(url, timeout=10)   
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        df = pd.read_csv(io.StringIO(response.text), sep=sep, encoding=encoding, names=names, skiprows=skiprows)
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
    
def get_soup_response_to_pd(url: str, header_class:str = None, table_class:str = None, headers:dict=None, data:dict=None,timeout:int=None) ->pd.DataFrame:
    if timeout is None:
        timeout= 10
    try:
        response = requests.get(url, headers=headers, data=data, timeout=timeout)         
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.text, 'html.parser')
        header_table = soup.find('table', class_=f'{header_class}')
        headers = []
        if header_table:
            header_tds = header_table.find_all('td')
            for td in header_tds:
                text = td.get_text(strip=True)
                if text:
                    headers.append(text)
        else:
            print(f"Could not find the header table (class='{header_class}').")
            headers = ["Data de Negociação", "Volume Negociado em Moeda da Época"]

        data_table = soup.find('table', class_=f'{table_class}')
        extracted_data = []

        if data_table:
            rows = data_table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.get_text(strip=True) for ele in cols]
                cleaned_row = [col for col in cols if col]

                if cleaned_row:
                    if "Total:" in cleaned_row[0] or "Total:" in cleaned_row[1]: 
                        if len(cleaned_row) == 1 and "Total:" in cleaned_row[0]:
                            total_value_str = cleaned_row[0].replace("Total:", "").strip()
                            extracted_data.append(["Total", total_value_str])
                        elif len(cleaned_row) == 2 and "Total:" in cleaned_row[1]:
                            total_value_str = cleaned_row[1].replace("Total:", "").strip()
                            extracted_data.append(["Total", total_value_str])
                        else: 
                            extracted_data.append(cleaned_row)
                    elif len(cleaned_row) == 2: 
                        extracted_data.append(cleaned_row)
        else:
            print(f"Could not find the data table (class='{table_class}').")
        if extracted_data and headers:
            df = pd.DataFrame(extracted_data, columns=headers)
            #print("\nDataFrame created successfully:")
        elif extracted_data:
            df = pd.DataFrame(extracted_data)
            print("\nDataFrame created successfully (without specific headers):")
        else:
            df = pd.DataFrame()
            print("No data extracted to form a DataFrame.")
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
