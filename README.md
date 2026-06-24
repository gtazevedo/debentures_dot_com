# debentures_dot_com

API para consultar dados do site [debentures.com.br](https://www.debentures.com.br/).

Esta API está atualmente em sua versão inicial; portanto, algumas funções ainda estão incompletas e nem todos os dados podem ser consultados no momento.

Atualmente, foram adicionadas consultas para os seguintes dados disponíveis na seção [Consulta a Dados](https://www.debentures.com.br/exploreosnd/consultaadados):

- **Emissões Debêntures** (acessível através da classe `EmissoesDebentures`)
- **Estoques Corporativos** (acessível através da classe `EstoquesCorporativos`)
- **Eventos Financeiros** (acessível através da classe `EventosFinanceiros`)
- **Mercado Secundário** (acessível através da classe `MercadoSecundario`)

## Instalação
O pacote pode ser instalado via pip (exemplo):
```bash
pip install debentures-dot-com
```

## Como Usar
Um exemplo rápido de como consultar emissões de debêntures públicas:

```python
from debentures_dot_com import EmissoesDebentures

ed = EmissoesDebentures()
df = ed.lista_deb_publicas()
print(df.head())
```

Para mais detalhes e testes de desenvolvimento, você pode verificar o arquivo `tests/dev.ipynb`.