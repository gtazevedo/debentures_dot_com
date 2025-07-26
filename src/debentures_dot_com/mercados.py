import requests
import io
from dateutil import parser
import pandas as pd
from utils.utils import get_response_to_pd, _format_cnpj, _format_date_for_url,simple_response_to_pd,get_soup_response_to_pd
from __consulta_dados import UrlDebentures

class MercadoSecundario:
    def __init__(self)->str:
        root_url = UrlDebentures().root_url
        self.root_url = f'https://www.anbima.com.br/informacoes/merc-sec-debentures/'
        self.root_url_ = f'{root_url}/mercadosecundario'

    def arquivo_precos_diario(self, data:str)->pd.DataFrame:
        data_ = _format_date_for_url(data, '%y%m%d')
        url = f'{self.root_url}/arqs/db{data_}.txt'
        return simple_response_to_pd(url, '@')

    #def vencidos_antecipadamente_diario(self, data:str)->pd.DataFrame:
    #    data_ = _format_date_for_url(data, '%d%b%Y')
    #    url = f'{self.root_url}/resultados/mdeb_{data_}_vencidos_antecipadamente.asp'
    #    df = get_response_to_pd(url)
    #    return df

    #def ipca_spread_diario(self, data:str)->pd.DataFrame:
    #    data_ = _format_date_for_url(data, '%d%b%Y')
    #    url = f'{self.root_url}/resultados/mdeb_{data_}_ipca_spread.asp'
    #    df = get_response_to_pd(url)
    #    return df
    
    #def igpm_spread_diario(self, data:str)->pd.DataFrame:
    #    data_ = _format_date_for_url(data, '%d%b%Y')
    #    url = f'{self.root_url}/resultados/mdeb_{data_}_igp-m.asp'
    #    df = get_response_to_pd(url)
    #    return df
    
    def preco_negociacao(self, ativo:str = None, exec:str = None,emissor:str = None, dt_ini:str=None, dt_fim:str=None)->list:
        ativo = ativo if isinstance(ativo, str) else ''
        emissor = _format_cnpj(emissor) if emissor else ''
        exec = exec if isinstance(exec, str) else 'Nada'
        dt_ini = _format_date_for_url(dt_ini, '%Y%m%d') if dt_ini else '19900302'
        dt_fim = _format_date_for_url(dt_fim, '%Y%m%d') if dt_fim else '20291231'
        url = (
            f'{self.root_url_}/precosdenegociacao_e.asp?'
            f'op_exc={exec}&emissor={emissor}&ativo={ativo}&dt_ini={dt_ini}&dt_fim={dt_fim}'
        )
        df = get_response_to_pd(url)
        return df

    def volume_negociacao(self, dt_ini:str=None, dt_fim:str=None)->list:
        dt_ini = _format_date_for_url(dt_ini, '%d/%m/%Y') if dt_ini else '02/03/1992'
        dt_fim = _format_date_for_url(dt_fim, '%d/%m/%Y') if dt_fim else '31/12/2029'

        url = f'{self.root_url_}/volumesnegociados_r.asp'

        headers = {
            "Host": "www.debentures.com.br",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.debentures.com.br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "DNT": "1",
            "Sec-GPC": "1",
            "Priority": "u=0, i",
        }

        data = {
            "dt_ini": f"{dt_ini}",
            "dt_fim": f"{dt_fim}", 
        }

        df = get_soup_response_to_pd(url, header_class='Ver10666666_cab', table_class='Tab10333333', headers=headers, data=data)
        return df
