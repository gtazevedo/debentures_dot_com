import requests
import io
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import date


class EmissoesDebentures:
    def __init__(self, root_url):
        self.root_url = f'{root_url}/emissoesdedebentures'

    def lista_deb_publicas(self):
        url = f'{self.root_url}/caracteristicas_r.asp?tip_deb=publicas&op_exc='
        r = requests.get(url)
        soup = BeautifulSoup(r.text)
        table = soup.find('table', class_='Tab10333333')
        # Check if the table exists
        if table:
            df = []
            # Extract table rows
            rows = table.find_all('tr')
            # Loop through rows and extract data
            for row in rows:
                cells = row.find_all(['td', 'th'])  # handles both header and data cells
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                df.append(cell_texts[1:-1])
            df = pd.DataFrame(df, columns = ['Ativo', 'Emissor', 'Dump', 'Situacao'])
            df = df.drop(columns=['Dump'])
        else:
            df = pd.DataFrame()
            print("Table with class 'Tab10333333' not found.")
        return df
    
    def lista_caracteristicas(self, ativo):
        url = f'{self.root_url}/caracteristicas_e.asp?Ativo={ativo}'
        r = requests.get(url)
        df = pd.read_csv(io.StringIO(r.text), sep='|', encoding='utf-8', names=['raw'], skiprows=2)
        df = df[1:]['raw'].str.split('\t', expand=True).reset_index(drop=True)
        df = df.T.reset_index(drop=True)
        df.columns = ['Descricao', 'Valores']
        return df
    
    def _dt_fim_ini_fix(self, date_):
        dt_par = parser.parse(date_)
        return f'{dt_par.day:02d}%2F{dt_par.month:02d}%2F{dt_par.year}'

    
    def pu_historico(self, ativo, dt_inicio=None, dt_fim=None):
        params = []
        
        if not dt_inicio:
            dt_inicio= '20010101'
        
        if not dt_fim:
            dt_fim = date.today().strftime('%Y%m%d')

        
        dt_inicio_fmt = self._dt_fim_ini_fix(dt_inicio)
        params.append(f'dt_ini={dt_inicio_fmt}')
        dt_fim_fmt = self._dt_fim_ini_fix(dt_fim)
        params.append(f'dt_fim={dt_fim_fmt}')
        
        # Add conditional suffix if any date filters exist
        add_suffix = '++++' if params else ''
        
        query_string = '&' + '&'.join(params) if params else ''
        url = f'{self.root_url}/puhistorico_e.asp?op_exc=False&ativo={ativo}{add_suffix}{query_string}'
        
        response = requests.get(url)
        df = pd.read_csv(io.StringIO(response.text), sep='|', encoding='utf-8', names=['raw'], skiprows=2)
        df = df['raw'].str.split('\t', expand=True).reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)[:-2]
        return df
    