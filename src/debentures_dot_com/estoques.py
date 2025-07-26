import re
import pandas as pd
import requests
from dateutil import parser
from .utils.utils import get_response_to_pd, _format_cnpj, _format_date_for_url,simple_response_to_pd,get_soup_response_to_pd
from __consulta_dados import UrlDebentures

def parse_estoque_data(data_string: str, tipo: str) -> pd.DataFrame:
    """
    Parses the given string content into a pandas DataFrame,
    now also extracting the 'Moeda' type.

    Args:
        data_string: A string containing the stock data with multiple date blocks.

    Returns:
        A pandas DataFrame with the extracted stock information.
    """
    lines = data_string.strip().split('\n')
    parsed_data = []
    current_date = None
    current_moeda = None # New variable to store the currency type

    for line in lines:
        line = line.strip()

        # Skip header lines that are not part of the data blocks
        if line.startswith("Estoque SND Caracter"):
            continue

        # Extract 'Data do Estoque' and 'Moeda'
        # Adjusted regex to capture the currency symbol (R$ or US$)
        date_moeda_match = re.match(r'Data do Estoque (\d{2}/\d{2}/\d{4}) - Moeda (R\$|US\$)', line)
        if date_moeda_match:
            current_date = pd.to_datetime(date_moeda_match.group(1), format='%d/%m/%Y')
            current_moeda = date_moeda_match.group(2) # Capture the currency type
            continue

        # Identify header row for data blocks
        if current_date and line.startswith(tipo):
            headers = [h.strip() for h in line.split('\t') if h.strip()]
            continue # Move to the next line to read data

        # Read data rows (excluding "Total do dia")
        if current_date and not line.startswith("Total do dia") and not line.startswith(tipo) and line:
            parts = [p.strip() for p in line.split('\t') if p.strip()]
            if parts and len(parts) == len(headers):
                row_dict = {'Data do Estoque': current_date, 'Moeda': current_moeda} # Add 'Moeda' to the dictionary
                for i, header in enumerate(headers):
                    value = parts[i]
                    # Clean and convert numeric values
                    if header != tipo:
                        value = value.replace('.', '').replace(',', '.') # Remove thousands separator, change decimal comma to dot
                        try:
                            value = float(value)
                        except ValueError:
                            pass # Keep as string if not a number
                    row_dict[header] = value
                parsed_data.append(row_dict)

    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)

    # Ensure correct column order and data types
    if not df.empty:
        df['Data do Estoque'] = pd.to_datetime(df['Data do Estoque'])
        # Convert numeric columns, handling potential errors if any non-numeric data slipped through
        for col in ['Mercado', 'Tesouraria', 'Total']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def get_estoque_to_pd(url:str, tipo: str, timeout:int=None)-> pd.DataFrame:
    timeout = timeout if isinstance(timeout, int) else 10
    try:
        response = requests.get(url, timeout=timeout)
        response.encoding = 'ISO-8859-1'  
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        return parse_estoque_data(response.text, tipo)
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

def _consulta_relatorio(url:str, tipo_relatorio:str, dt_ini:str=None, dt_fim:str=None, moeda:int=None,opcao:int=None,exec:str=None,timeout:int=None)->pd.DataFrame:
    dt_ini = _format_date_for_url(dt_ini, '%d/%m/%Y') if dt_ini else '02/03/1992'
    dt_fim = _format_date_for_url(dt_fim,'%d/%m/%Y') if dt_fim else '31/12/2029'
    if moeda is not None and moeda not in [1, 2]:
        raise ValueError("Parameter 'moeda' must be 1 or 2.")
    moeda = moeda if isinstance(moeda, int) else 1
    exec = exec if isinstance(exec, str) else 'on'
    opcao = opcao if isinstance(opcao, int) else 100
    url = (
        f'{url}/estoquepor_re.asp?op_rel={tipo_relatorio}'
        f'&Dt_ini={dt_ini}&Dt_fim={dt_fim}&op_exc={exec}&op_subInd=&Opcao={opcao}&Moeda={moeda}'
    )
    return get_estoque_to_pd(url, tipo_relatorio, 1000)

class EstoquesCorporativos:
    """
        Consult the open data page on debentures.com, 
        specifically the tab 'estoque' (https://www.debentures.com.br/exploreosnd/consultaadados/estoque/) 
        to extract the information via Python.
    """

    def __init__(self)->str:
        root_url = UrlDebentures().root_url
        self.root_url = f'{root_url}/estoque'

    def estoque_por_ativo(self, ativo:str=None, dt_ini:str=None, dt_fim:str=None, exec:str=None,moeda:int=None,timeout:int=None)->pd.DataFrame:
        ativo = ativo if isinstance(ativo, str) else ''
        exec = exec if isinstance(exec, str) else 'Nada'
        if moeda is not None and moeda not in [1, 2]:
            raise ValueError("Parameter 'moeda' must be 1 or 2.")
        moeda = moeda if isinstance(moeda, int) else 1
        dt_ini = _format_date_for_url(dt_ini, '%d/%m/%Y') if dt_ini else '02/03/1992'
        dt_fim = _format_date_for_url(dt_fim, '%d/%m/%Y') if dt_fim else '31/12/2029'
        url = (
            f'{self.root_url}/estoqueporativo_e.asp?'
            f'ativo={ativo}&dt_ini={dt_ini}&dt_fim={dt_fim}&moeda={moeda}&Op_exc={exec}'
        )
        if moeda == 1:
            simb_moeda= 'R$'
        else:
            simb_moeda= 'USD'
        df = get_response_to_pd(url,skiprows=4,timeout=timeout)
        #df = df.iloc[2:]
        df.columns = ['Data', 'Qtd Mercado', f'Volume Mercado ({simb_moeda} Mil)','Qtd Tesouraria', 
                      f'Volume Tesouraria ({simb_moeda} Mil)','Qtd Total', 
                      f'Volume Total ({simb_moeda} Mil)', 'Dump']
        df = df.drop(columns=['Dump'])
        return df
    
    def estoque_por_periodo(self, dt_ini:str=None, dt_fim:str=None, moeda:int=None,timeout:int=None)->pd.DataFrame:
        if moeda is not None and moeda not in [1, 2]:
            raise ValueError("Parameter 'moeda' must be 1 or 2.")
        moeda = moeda if isinstance(moeda, int) else 1
        dt_ini = _format_date_for_url(dt_ini) if dt_ini else '02/03/1992'
        dt_fim = _format_date_for_url(dt_fim) if dt_fim else '31/12/2029'
        url = (
            f'{self.root_url}/estoqueporperiodo_e.asp?'
            f'dt_ini={dt_ini}&dt_fim={dt_fim}&moeda={moeda}'
        )
        df = get_response_to_pd(url,timeout=timeout)
        return df.iloc[:,:-1]
    
    def estoque_a_vencer(self, dt_ini:str=None, dt_fim:str=None, moeda:int=None,repactuacao:int=None,timeout:int=None)->pd.DataFrame:
        dt_ini = _format_date_for_url(dt_ini) if dt_ini else '02/03/1992'
        dt_fim = _format_date_for_url(dt_fim) if dt_fim else '31/12/2029'
        if moeda is not None and moeda not in [1, 2]:
            raise ValueError("Parameter 'moeda' must be 1 or 2.")
        moeda = moeda if isinstance(moeda, int) else 1
        if repactuacao is not None and repactuacao not in [1, 2]:
            raise ValueError("Parameter 'moeda' must be 1 or 2.")
        repactuacao = repactuacao if isinstance(repactuacao, int) else 1
        url = (
            f'{self.root_url}/estoqueavencer_e.asp?'
            f'dt_ini={dt_ini}&dt_fim={dt_fim}&moeda={moeda}&rVen={repactuacao}'
        )
        df = get_response_to_pd(url,skiprows=4,timeout=timeout)
        return df.iloc[:,:-1]
    
    def estoque_relatorio(self, tipo:str, dt_ini:str=None, dt_fim:str=None, moeda:int=None,opcao:int=None,exec:str=None,timeout:int=None)->pd.DataFrame:
        if tipo not in ['Indexadores', 'Tipo', 'Forma', 'Classe','Garantia','InstrucaoNormativa']:
            raise ValueError("Parameter 'tipo' must be 'Indexadores', 'Tipo', 'Forma', 'Classe', 'Garantia' or 'InstrucaoNormativa'")
        return _consulta_relatorio(self.root_url, tipo, dt_ini, dt_fim, moeda,opcao,exec,timeout)

