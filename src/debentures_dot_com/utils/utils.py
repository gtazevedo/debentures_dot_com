import requests
import io
import pandas as pd

def get_response_to_pd(url:str)->pd.DataFrame:
    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text), sep='|', encoding='utf-8', names=['raw'], skiprows=2)
    df = df['raw'].str.split('\t', expand=True).reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)[:-2]
    return df