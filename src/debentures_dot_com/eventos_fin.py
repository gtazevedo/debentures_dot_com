import requests
import io
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import date
from utils.utils import get_response_to_pd, _format_cnpj, _format_date_for_url

class EventosFinanceiros:
    def __init__(self, root_url:str)->str:
        self.root_url = f'{root_url}/eventosfinanceiros'

    def agenda_eventos(self, ativo:str = None, emissor:str = None, evento:str = None, dt_ini:str=None, dt_fim:str=None, dt_pgto_ini:str=None, dt_pgto_fim:str=None)->list:
        ativo = ativo if isinstance(ativo, str) else ''
        emissor = _format_cnpj(emissor) if emissor else ''
        evento = evento if isinstance(evento, str) else ''
        dt_ini = _format_date_for_url(dt_ini)
        dt_fim = _format_date_for_url(dt_fim)
        dt_pgto_ini = _format_date_for_url(dt_pgto_ini)
        dt_pgto_fim = _format_date_for_url(dt_pgto_fim)
        url = (
            f'{self.root_url}/agenda_e.asp?'
            f'emissor={emissor}&ativo={ativo}&evento={evento}&dt_ini={dt_ini}&dt_fim={dt_fim}'
            f'&dt_pgto_ini={dt_pgto_ini}&dt_pgto_fim={dt_pgto_fim}'
        )
        df = get_response_to_pd(url)
        return df
    
    def pu_eventos(self, ativo:str = None, exec:str = None,emissor:str = None, evento:str = None, dt_ini:str=None, dt_fim:str=None)->list:
        ativo = ativo if isinstance(ativo, str) else ''
        emissor = _format_cnpj(emissor) if emissor else ''
        exec = exec if isinstance(exec, str) else 'Nada'
        evento = evento if isinstance(evento, str) else ''
        dt_ini = _format_date_for_url(dt_ini)
        dt_fim = _format_date_for_url(dt_fim)
        url = (
            f'{self.root_url}/pudeeventos_e.asp?'
            f'op_exc={exec}&ativo={ativo}&evento={evento}&dt_ini={dt_ini}&dt_fim={dt_fim}&emissor={emissor}'
        )
        df = get_response_to_pd(url)
        return df