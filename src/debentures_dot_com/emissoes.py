import requests
import io
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import date
from utils.utils import get_response_to_pd, _format_cnpj, _format_date_for_url

class EmissoesDebentures:
    def __init__(self, root_url:str)->str:
        self.root_url = f'{root_url}/emissoesdedebentures'

    def lista_deb_publicas(self)->pd.DataFrame:
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
    
    def lista_caracteristicas(self, ativo:str)->pd.DataFrame:
        url = f'{self.root_url}/caracteristicas_e.asp?Ativo={ativo}'
        r = requests.get(url)
        df = pd.read_csv(io.StringIO(r.text), sep='|', encoding='utf-8', names=['raw'], skiprows=2)
        df = df[1:]['raw'].str.split('\t', expand=True).reset_index(drop=True)
        df = df.T.reset_index(drop=True)
        df.columns = ['Descricao', 'Valores']
        return df
    
    #def _dt_fim_ini_fix(self, date_):
    #    dt_par = parser.parse(date_)
    #    return f'{dt_par.day:02d}%2F{dt_par.month:02d}%2F{dt_par.year}'
    
    def pu_historico(self, ativo:str, dt_inicio:str=None, dt_fim:str=None)->pd.DataFrame:
        params = []
        
        if not dt_inicio:
            dt_inicio= '20010101'
        
        if not dt_fim:
            dt_fim = date.today().strftime('%Y%m%d')

        
        dt_inicio_fmt = _format_date_for_url(dt_inicio)
        params.append(f'dt_ini={dt_inicio_fmt}')
        dt_fim_fmt = _format_date_for_url(dt_fim)
        params.append(f'dt_fim={dt_fim_fmt}')
        
        # Add conditional suffix if any date filters exist
        add_suffix = '++++' if params else ''
        
        query_string = '&' + '&'.join(params) if params else ''
        url = f'{self.root_url}/puhistorico_e.asp?op_exc=False&ativo={ativo}{add_suffix}{query_string}'
        
        return get_response_to_pd(url)
    
    def prazo_medio(self, ativo:str = None, emissor:str = None, datacvm:str = None, dt_ini:str=None, dt_fim:str=None, anoini:str=None, anofim:str=None, repactuacao:str = None, exec:str = None)->list:
        ativo = ativo if isinstance(ativo, str) else ''
        emissor = _format_cnpj(emissor) if emissor else ''
        datacvm = datacvm if isinstance(datacvm, str) else 'e'
        dt_ini = _format_date_for_url(dt_ini)
        dt_fim = _format_date_for_url(dt_fim)
        repactuacao = repactuacao if isinstance(repactuacao, str) else ''
        exec = exec if isinstance(exec, str) else 'Nada'
        if isinstance(anoini, int):
            anoini = str(anoini)
        elif not isinstance(anoini, str):
            anoini = ''

        if isinstance(anofim, int):
            anofim = str(anofim)
        elif not isinstance(anofim, str):
            anofim = ''
        url = (
            f'{self.root_url}/prazo-medio_e.asp?'
            f'Ativo={ativo}&Emissor={emissor}&dataCVM={datacvm}&dt_ini={dt_ini}'
            f'&dt_fim={dt_fim}&anoini={anoini}&anofim={anofim}&ComRepactuacao={repactuacao}&Op_exc={exec}'
        )
        df = get_response_to_pd(url)
        df_medio = df[:2]
        df_medio = df_medio.iloc[:, :3]
        df_medio.columns = ['', 'Tipo Prazo', 'Prazo Medio']
        df_medio= df_medio.drop(columns='')
        df_data = df[2:].reset_index(drop=True)
        df_data.columns = df_data.iloc[0]
        df_data = df_data[1:].reset_index(drop=True)[:-2]
        return df_medio, df_data

    def conversao_permuta(self, ativo:str=None, exec:bool=None, dt_ini:str=None, dt_fim:str=None, classe:str=None)->pd.DataFrame:
        ativo = ativo if isinstance(ativo, str) else ''
        dt_ini = _format_date_for_url(dt_ini)
        dt_fim = _format_date_for_url(dt_fim)
        classe = classe if isinstance(classe, str) else ''
        exec = exec if isinstance(exec, bool) else False
        url = (
            f'{self.root_url}/conversoes-permutas_e.asp?'
            f'ativo={ativo},%20&op_exc={exec}&dt_ini={dt_ini}&dt_fim={dt_fim}&classe={classe}'
        )
        return get_response_to_pd(url)
    
    def caracteristicas_debs(self, tipo:str=None,exec:bool=None,mnome:str=None,ativo:str=None,
                             ipo:str=None,icvm:str=None,escri_padro:str=None,cvm_ini:str=None,cvm_fim:str=None,
                             emis_ini:str=None,emis_fim:str=None,venc_ini:str=None,venc_fim:str=None,tpv:str=None,
                             tnv:str=None,rent_ini:str=None,rent_fim:str=None,distrib_ini:str=None,distrib_fim:str=None,
                             indice:str=None,tipo_:str=None,crit_calc:str=None,dia_ref:str=None,mult_rend:str=None,
                             limite:str=None,trat_limite:str=None,tx_spread:str=None,prazo:str=None,premio_novo:str=None,
                             premio_prazo:str=None,premio_antigo:str=None,par:str=None,amortizacao:str=None,mbanco:str=None,
                             magente:str=None,instdep:str=None,coordenador:str=None)->pd.DataFrame:
        tipo = tipo if isinstance(tipo, str) else 'privadas'
        exec = exec if isinstance(exec, bool) else False
        mnome = mnome if isinstance(mnome, str) else ''
        ativo = ativo if isinstance(ativo, str) else ''
        ipo = ipo if isinstance(ipo, str) else ''
        icvm = icvm if isinstance(icvm, str) else ''
        escri_padro = escri_padro if isinstance(escri_padro, str) else ''
        cvm_ini = cvm_ini if isinstance(cvm_ini, str) else ''
        cvm_fim = cvm_fim if isinstance(cvm_fim, str) else ''
        emis_ini = emis_ini if isinstance(emis_ini, str) else ''
        emis_fim = emis_fim if isinstance(emis_fim, str) else ''
        venc_ini = venc_ini if isinstance(venc_ini, str) else ''
        venc_fim = venc_fim if isinstance(venc_fim, str) else ''
        tpv = tpv if isinstance(tpv, str) else ''
        tnv = tnv if isinstance(tnv, str) else ''
        rent_ini = rent_ini if isinstance(rent_ini, str) else ''
        rent_fim = rent_fim if isinstance(rent_fim, str) else ''
        distrib_ini = distrib_ini if isinstance(distrib_ini, str) else ''
        distrib_fim = distrib_fim if isinstance(distrib_fim, str) else ''
        indice = indice if isinstance(indice, str) else ''
        tipo_ = tipo_ if isinstance(tipo_, str) else ''
        crit_calc = crit_calc if isinstance(crit_calc, str) else ''
        dia_ref = dia_ref if isinstance(dia_ref, str) else ''
        mult_rend = mult_rend if isinstance(mult_rend, str) else ''
        limite = limite if isinstance(limite, str) else ''
        trat_limite = trat_limite if isinstance(trat_limite, str) else ''
        tx_spread = tx_spread if isinstance(tx_spread, str) else ''
        prazo = prazo if isinstance(prazo, str) else ''
        premio_novo = premio_novo if isinstance(premio_novo, str) else ''
        premio_prazo = premio_prazo if isinstance(premio_prazo, str) else ''
        premio_antigo = premio_antigo if isinstance(premio_antigo, str) else ''
        par = par if isinstance(par, str) else ''
        amortizacao = amortizacao if isinstance(amortizacao, str) else ''
        mbanco = mbanco if isinstance(mbanco, str) else ''
        magente = magente if isinstance(magente, str) else ''
        instdep = instdep if isinstance(instdep, str) else ''
        coordenador = coordenador if isinstance(coordenador, str) else ''

        url = (
            f'{self.root_url}/caracteristicas_e.asp?tip_deb={tipo}&'
            f'op_exc={exec}&mnome={mnome}&ativo={ativo}&IPO={ipo}&icvm={icvm}&EscrituraPadronizada={escri_padro}&'
            f'cvm_ini={cvm_ini}&cvm_fim={cvm_fim}&emis_ini={emis_ini}&emis_fim={emis_fim}&venc_ini={venc_ini}&venc_fim={venc_fim}&TPV={tpv}&'
            f'TNV={tnv}&rent_ini={rent_ini}&rent_fim={rent_fim}&distrib_ini={distrib_ini}&distrib_fim={distrib_fim}&indice={indice}&'
            f'tipo={tipo_}&crit_calc={crit_calc}&dia_ref={dia_ref}&mult_rend={mult_rend}&limite={limite}&trat_limite={trat_limite}&'
            f'tx_spread={tx_spread}&prazo={prazo}&premio_novo={premio_novo}&premio_prazo={premio_prazo}&premio_antigo={premio_antigo}&Par={par}&'
            f'amortizacao={amortizacao}&mbanco={mbanco}&magente={magente}&instdep={instdep}&coordenador={coordenador}'
        )
        df = get_response_to_pd(url)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)[:-2]
        return df
