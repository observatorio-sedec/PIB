import datetime
from pathlib import Path
import pandas as pd
import requests as rq
import ssl
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
        6579: ['variavel9324'],
    }

    if tabela_id in variaveis_por_tabela:
        variaveis = variaveis_por_tabela[tabela_id]

        if dados_brutos:
            return tuple(dados_brutos[i] if i < len(dados_brutos) else None for i in range(len(variaveis)))
        else:
            return tuple([None] * len(variaveis))
    
    return None

def tratando_dados5938(variavel9324):
    print(variavel9324)
    dados_limpos_9324 = []

    id_tabela = variavel9324[0]['id']
    variavel = variavel9324[0]['variavel']
    unidade = variavel9324[0]['unidade']
    dados = variavel9324[0]['resultados']

    for ii in dados:
        dados_produto = ii['classificacoes']
        dados_producao = ii['series']

        for iv in dados_producao:
            id = iv['localidade']['id']
            local = iv['localidade']['nome']
            dados_ano_producao = iv['serie']
            

            for ano, producao in dados_ano_producao.items():
                producao = producao.replace('-', '0').replace('...', '0')
                
                
                dict = {
                    'codigo': id,
                    'unidade_federativa': local,
                    'referencia': f'01/01/{ano}',
                    variavel: producao
            }
                if id_tabela == '9324':
                    dados_limpos_9324.append(dict)
                                
    return dados_limpos_9324


def gerando_dataframe(dados_limpos_9324):
    df_pop = pd.DataFrame(dados_limpos_9324)
    return df_pop

ano_atual = datetime.datetime.now().year

def executando_dados(tabela):
    lista_dados_9324 = [] 
    for ano in range(2002, ano_atual):
        if ano in (2007, 2010, 2022, 2023):  
            pass
        else:
            api = f'https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/{ano}/variaveis/9324?localidades=N3[all]'
            variavel9324 = extrair_dados(api, tabela)
            if all(v is None for v in [variavel9324]):
                break
            else:
                novos_dados_9324 = tratando_dados5938(variavel9324)
                lista_dados_9324.extend(novos_dados_9324)
    
    return  lista_dados_9324


dados_limpos_9324 = executando_dados(6579)
df_pop = gerando_dataframe(dados_limpos_9324)
df_pop.to_excel(ROOT_PATH / 'dados_pop_estadual.xlsx')


if __name__ == '__main__':
    from sql import executar_sql 
    executar_sql()

