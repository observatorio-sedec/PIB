import datetime
from pathlib import Path
import polars as pl
import requests as rq
import ssl
from concurrent.futures import ThreadPoolExecutor
from ajustar_planilha import ajustar_bordas, ajustar_colunas

ROOT_PATH = Path(__file__).parent

class TLSAdapter(rq.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   # OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

def requisitando_dados(api):
    with rq.session() as s:
        s.mount("https://", TLSAdapter())
        dados_brutos_api = s.get(api, verify=True)

    if dados_brutos_api.status_code != 200:
        raise Exception(f"A solicitação à API falhou com o código de status: {dados_brutos_api.status_code}")

    try:
        dados_brutos = dados_brutos_api.json()
    except Exception as e:
        raise Exception(f"Erro ao analisar a resposta JSON da API: {str(e)}")
    return dados_brutos

def extrair_dados(api, tabela_id):
    dados_brutos = requisitando_dados(api)

    variaveis_por_tabela = {
        5938: ['variavel37', 'variavel543', 'variavel498', 'variavel513', 'variavel517', 'variavel6575', 'variavel525'],
        5939: ['variavel529', 'variavel531', 'variavel532', 'variavel6568', 'variavel534']
    }

    if tabela_id in variaveis_por_tabela:
        variaveis = variaveis_por_tabela[tabela_id]
        if dados_brutos:
            return tuple(dados_brutos[i] if i < len(dados_brutos) else None for i in range(len(variaveis)))
        else:
            return tuple([None] * len(variaveis))
    
    return None

def tratando_dados5938(variavel37, variavel543, variavel498, variavel513, variavel517, variavel6575, variavel525):
    listas = [[] for _ in range(7)]
    variaveis = [variavel37, variavel543, variavel498, variavel513, variavel517, variavel6575, variavel525]
    
    id_map = {'37': 0, '543': 1, '498': 2, '513': 3, '517': 4, '6575': 5, '525': 6}

    for i in variaveis:
        id_tabela = i['id']
        var_name = i['variavel']
        unidade = i['unidade']
        for ii in i['resultados']:
            for iv in ii['series']:
                local_id = iv['localidade']['id']
                local_nome = iv['localidade']['nome']
                for ano, producao in iv['serie'].items():
                    producao = producao.replace('-', '0').replace('...', '0')
                    row = {
                        'id': local_id,
                        'local': local_nome,
                        var_name: producao,
                        'unidade': unidade,
                        'ano': f'01/01/{ano}'
                    }
                    if id_tabela in id_map:
                        listas[id_map[id_tabela]].append(row)
                                
    return tuple(listas)

def gerando_dataframe5938(listas):
    dfs = [pl.DataFrame(l) for l in listas]
    join_cols = ['id', 'local', 'unidade', 'ano']
    
    dataframe = dfs[0]
    for df in dfs[1:]:
        dataframe = dataframe.join(df, on=join_cols, how='inner')
    
    cols_to_cast = [
        'Produto Interno Bruto a preços correntes', 
        'Impostos, líquidos de subsídios, sobre produtos a preços correntes', 
        'Valor adicionado bruto a preços correntes total', 
        'Valor adicionado bruto a preços correntes da agropecuária', 
        'Valor adicionado bruto a preços correntes da indústria', 
        'Valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social', 
        'Valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social'
    ]
    
    dataframe = dataframe.with_columns([
        (pl.col(c).cast(pl.Float64) * 1000) for c in cols_to_cast
    ]).with_columns(
        pl.col('local').str.replace(r'\s*\(MT\)', '')
    )
    
    return dataframe

def tratando_dados5939(variavel529, variavel531, variavel532, variavel6568, variavel534):
    listas = [[] for _ in range(5)]
    variaveis = [variavel529, variavel531, variavel532, variavel6568, variavel534]
    id_map = {'529': 0, '531': 1, '532': 2, '6568': 3, '534': 4}

    for i in variaveis:
        id_tabela = i['id']
        var_name = i['variavel']
        unidade = i['unidade']
        for ii in i['resultados']:
            for iv in ii['series']:
                local_id = iv['localidade']['id']
                local_nome = iv['localidade']['nome']
                for ano, producao in iv['serie'].items():
                    producao = producao.replace('-', '0').replace('...', '0')
                    row = {
                        'id': local_id,
                        'local': local_nome,
                        'ano': f'01/01/{ano}',
                        var_name: producao,
                        'unidade': unidade
                    }
                    if id_tabela in id_map:
                        listas[id_map[id_tabela]].append(row)
                                
    return tuple(listas)

def gerando_dataframe5939(listas):
    dfs = [pl.DataFrame(l) for l in listas]
    join_cols = ['id', 'local', 'unidade', 'ano']
    
    dataframe = dfs[0]
    for df in dfs[1:]:
        dataframe = dataframe.join(df, on=join_cols, how='inner')
    
    cols_to_cast = [
        'Índice de Gini da distribuição do produto interno bruto a preços correntes', 
        'Índice de Gini da distribuição do valor adicionado bruto a preços correntes da agropecuária', 
        'Índice de Gini da distribuição do valor adicionado bruto a preços correntes da indústria', 
        'Índice de Gini da distribuição do valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social', 
        'Índice de Gini da distribuição do valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social'
    ]
    
    dataframe = dataframe.with_columns([
        pl.col(c).cast(pl.Float64) for c in cols_to_cast
    ]).rename({'ano': 'data'})
    
    return dataframe

ano_atual = datetime.datetime.now().year

def executando_dados(tabela, muni):
    def processar_ano(ano):
        if muni:
            api = f'https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/{ano}/variaveis/37|543|498|513|517|6575|525?localidades=N6[5100102,5100201,5100250,5100300,5100359,5100409,5100508,5100607,5100805,5101001,5101209,5101258,5101308,5101407,5101605,5101704,5101803,5101852,5101902,5102504,5102603,5102637,5102678,5102686,5102694,5102702,5102793,5102850,5103007,5103056,5103106,5103205,5103254,5103304,5103353,5103361,5103379,5103403,5103437,5103452,5103502,5103601,5103700,5103809,5103858,5103908,5103957,5104104,5104203,5104500,5104526,5104542,5104559,5104609,5104807,5104906,5105002,5105101,5105150,5105176,5105200,5105234,5105259,5105309,5105507,5105580,5105606,5105622,5105903,5106000,5106109,5106158,5106174,5106182,5106190,5106208,5106216,5106224,5106232,5106240,5106257,5106265,5106273,5106281,5106299,5106307,5106315,5106372,5106422,5106455,5106505,5106653,5106703,5106752,5106778,5106802,5106828,5106851,5107008,5107040,5107065,5107107,5107156,5107180,5107198,5107206,5107248,5107263,5107297,5107305,5107354,5107404,5107578,5107602,5107701,5107743,5107750,5107768,5107776,5107792,5107800,5107859,5107875,5107883,5107909,5107925,5107941,5107958,5108006,5108055,5108105,5108204,5108303,5108352,5108402,5108501,5108600,5108808,5108857,5108907,5108956]'
        else:
            api = f'https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/{ano}/variaveis/37|543|498|513|517|6575|525?localidades=N3[all]'
        
        try:
            extraidos = extrair_dados(api, tabela)
            if extraidos and any(v is not None for v in extraidos):
                return tratando_dados5938(*extraidos)
        except Exception as e:
            print(f"Erro ao processar ano {ano}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        resultados = list(executor.map(processar_ano, range(2002, ano_atual)))

    listas_finais = [[] for _ in range(7)]
    for res in resultados:
        if res:
            for i in range(7):
                listas_finais[i].extend(res[i])
    return tuple(listas_finais)

def executando_dados_2(tabela):
    def processar_ano_2(ano):
        api = f'https://servicodados.ibge.gov.br/api/v3/agregados/5939/periodos/{ano}/variaveis/529|531|532|6568|534?localidades=N3[all]'
        try:
            extraidos = extrair_dados(api, tabela)
            if extraidos and any(v is not None for v in extraidos):
                return tratando_dados5939(*extraidos)
        except Exception as e:
            print(f"Erro ao processar ano {ano}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        resultados = list(executor.map(processar_ano_2, range(2002, ano_atual)))

    listas_finais = [[] for _ in range(5)]
    for res in resultados:
        if res:
            for i in range(5):
                listas_finais[i].extend(res[i])
    return tuple(listas_finais)

# Definição global para importação no sql.py
listas_muni = executando_dados(5938, True)
df_pib_muni = gerando_dataframe5938(listas_muni)

listas_estadual = executando_dados(5938, False)
df_pib_estadual = gerando_dataframe5938(listas_estadual)

listas_gini = executando_dados_2(5939)
df_gini = gerando_dataframe5939(listas_gini)

if __name__ == '__main__':
    from sql import executar_sql 
    executar_sql()
