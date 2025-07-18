import pandas as pd
from utils.utils import get_response_to_pd, _format_cnpj, _format_date_for_url
from __consulta_dados import UrlDebentures

class MercadoSecundario:
    def __init__(self)->str:
        root_url = UrlDebentures().root_url
        self.root_url = f'{root_url}/merc-sec-debentures'
        self.root_url_ = f'{root_url}/mercadosecundario'

    def arquivo_precos_diario(self, data:str)->pd.DataFrame:
        data_ = _format_date_for_url(data, '%y%m%d')
        url = f'{self.root_url}/arqs/db{data_}.txt'
        df = get_response_to_pd(url)
        return df

    def vencidos_antecipadamente_diario(self, data:str)->pd.DataFrame:
        data_ = _format_date_for_url(data, '%d%b%Y')
        url = f'{self.root_url}/arqs/resultados/mdeb_{data_}_vencidos_antecipadamente.asp.txt'
        df = get_response_to_pd(url)
        return df

    def ipca_spread_diario(self, data:str)->pd.DataFrame:
        data_ = _format_date_for_url(data, '%d%b%Y')
        url = f'{self.root_url}/arqs/resultados/mdeb_{data_}_ipca_spread.asp'
        df = get_response_to_pd(url)
        return df
    
    def igpm_spread_diario(self, data:str)->pd.DataFrame:
        data_ = _format_date_for_url(data, '%d%b%Y')
        url = f'{self.root_url}/arqs/resultados/mdeb_{data_}_igp-m.asp'
        df = get_response_to_pd(url)
        return df
    
    def preco_negociacao(self, ativo:str = None, exec:str = None,emissor:str = None, evento:str = None, dt_ini:str=None, dt_fim:str=None)->list:
        ativo = ativo if isinstance(ativo, str) else ''
        emissor = _format_cnpj(emissor) if emissor else ''
        exec = exec if isinstance(exec, str) else 'False'
        evento = evento if isinstance(evento, str) else ''
        dt_ini = _format_date_for_url(dt_ini)
        dt_fim = _format_date_for_url(dt_fim)
        url = (
            f'{self.root_url_}/precosdenegociacao_e.asp?'
            f'op_exc={exec}&emissor={emissor}&ativo={ativo}&dt_ini={dt_ini}&dt_fim={dt_fim}'
        )
        df = get_response_to_pd(url)
        return df

