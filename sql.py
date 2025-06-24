import psycopg2
from ETL_pib import df_pib_muni,df_gini, df_pib_estadual
from população import df_pop
from conexão import conexao

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO pib, public')
    
    pib_municipal = \
    '''
    CREATE TABLE IF NOT EXISTS pib.pib_municipal (
        id_pib_municipal SERIAL PRIMARY KEY,
        id INTEGER,
        local TEXT,
        PIB_precos_correntes NUMERIC,
        unidade TEXT,
        ano DATE,
        impostos_liquidos_subsídios NUMERIC,
        VAB_total_precos_correntes NUMERIC,
        VAB_agropecuaria_precos_correntes NUMERIC,
        VAB_industria_precos_correntes NUMERIC,
        VAB_servicos_excluidos_precos_correntes NUMERIC,
        VAB_administracao_precos_correntes NUMERIC);
    '''
    pib_estadual = \
    '''
        CREATE TABLE IF NOT EXISTS pib.pib_estadual (
            id_pib_estadual SERIAL PRIMARY KEY,
            id INTEGER,
            local TEXT,
            PIB_precos_correntes NUMERIC,
            unidade TEXT,
            ano DATE,
            impostos_liquidos_subsídios NUMERIC,
            VAB_total_precos_correntes NUMERIC,
            VAB_agropecuaria_precos_correntes NUMERIC,
            VAB_industria_precos_correntes NUMERIC,
            VAB_servicos_excluidos_precos_correntes NUMERIC,
            VAB_administracao_precos_correntes NUMERIC);
    '''    
        
    gini_estadual = """
        CREATE TABLE IF NOT EXISTS pib.indice_gini_estadual (
            id TEXT,
            local TEXT,
            data_ano DATE,
            indice_gini_pib NUMERIC,
            unidade TEXT,
            indice_gini_agropecuaria NUMERIC,
            indice_gini_industria NUMERIC,
            indice_gini_servicos NUMERIC,
            indice_gini_administracao NUMERIC
        );
        """
        
    pop_estadual = \
    '''
        CREATE TABLE IF NOT EXISTS pib.pop_estadual (
            codigo TEXT,
            unidade_Territorial TEXT,
            referencia DATE,
            estimativa_populacional NUMERIC);
    '''

    cur.execute(pib_municipal)
    cur.execute(pib_estadual)
    cur.execute(gini_estadual)
    cur.execute(pop_estadual)
    
    verificando_existencia_pib_municipal = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pib' AND table_type='BASE TABLE' AND table_name='pib_municipal';
    '''
    verificando_existencia_pib_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pib' AND table_type='BASE TABLE' AND table_name='pib_estadual';
    '''
    
    cur.execute(verificando_existencia_pib_municipal)
    resultado_pib_municipal = cur.fetchone()
    
    cur.execute(verificando_existencia_pib_estadual)
    resultado_pib_estadual = cur.fetchone()
    
    verificando_existencia_gini_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pib' AND table_type='BASE TABLE' AND table_name='indice_gini_estadual';
    '''

    cur.execute(verificando_existencia_gini_estadual)
    resultado_gini = cur.fetchone()
    
    verificando_existencia_pop_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pib' AND table_type='BASE TABLE' AND table_name='pop_estadual';
    '''

    cur.execute(verificando_existencia_pop_estadual)
    resultado_pop = cur.fetchone()
    

    if resultado_pib_municipal[0] == 1:
        dropando_tabela_pib_municipal = '''
        TRUNCATE TABLE pib.pib_municipal;
        '''
        cur.execute(dropando_tabela_pib_municipal)
    else:
        pass
    if resultado_pib_estadual[0] == 1:
        dropando_tabela_pib_estadual = '''
        TRUNCATE TABLE pib.pib_estadual;
        '''
        cur.execute(dropando_tabela_pib_estadual)
    else:
        pass
    
    if resultado_gini[0] == 1:
        dropando_tabela_gini_estadual = '''
        TRUNCATE TABLE pib.indice_gini_estadual;
        '''
        cur.execute(dropando_tabela_gini_estadual)
    else:
        pass
    if resultado_pop[0] == 1:
        dropando_tabela_pop_estadual = '''
        TRUNCATE TABLE pib.pop_estadual;
        '''
        cur.execute(dropando_tabela_pop_estadual)
    else:
        pass

    #INSERINDO DADOS
    inserindo_pib_municipal= \
    '''
    INSERT INTO pib.pib_municipal (id, local, PIB_precos_correntes, unidade, ano, impostos_liquidos_subsídios, VAB_total_precos_correntes, VAB_agropecuaria_precos_correntes, VAB_industria_precos_correntes, VAB_servicos_excluidos_precos_correntes, VAB_administracao_precos_correntes)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df_pib_muni.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['Produto Interno Bruto a preços correntes'],
                i['unidade'],
                i['ano'],
                i['Impostos, líquidos de subsídios, sobre produtos a preços correntes'],
                i['Valor adicionado bruto a preços correntes total'],
                i['Valor adicionado bruto a preços correntes da agropecuária'],
                i['Valor adicionado bruto a preços correntes da indústria'],
                i['Valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social'],
                i['Valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social']
            )
            cur.execute(inserindo_pib_municipal, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados municipais: {e}")
        
    inserindo_pib_estadual = \
    '''
    INSERT INTO pib.pib_estadual (id, local, PIB_precos_correntes, unidade, ano, impostos_liquidos_subsídios, VAB_total_precos_correntes, VAB_agropecuaria_precos_correntes, VAB_industria_precos_correntes, VAB_servicos_excluidos_precos_correntes, VAB_administracao_precos_correntes)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df_pib_estadual.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['Produto Interno Bruto a preços correntes'],
                i['unidade'],
                i['ano'],
                i['Impostos, líquidos de subsídios, sobre produtos a preços correntes'],
                i['Valor adicionado bruto a preços correntes total'],
                i['Valor adicionado bruto a preços correntes da agropecuária'],
                i['Valor adicionado bruto a preços correntes da indústria'],
                i['Valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social'],
                i['Valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social']
            )
            cur.execute(inserindo_pib_estadual, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")
        
    inserindo_pop_estadual = \
    '''
    INSERT INTO pib.pop_estadual (codigo, unidade_Territorial, referencia, estimativa_Populacional)
    VALUES(%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df_pop.iterrows():
            dados = (
                i['codigo'],
                i['unidade_federativa'],
                i['referencia'],
                i['População residente estimada']
            )
            cur.execute(inserindo_pop_estadual, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados populacionais: {e}")
        
    inserindo_indice_gini = '''
    INSERT INTO indice_gini_estadual (id, local, data_ano, indice_gini_pib, unidade, indice_gini_agropecuaria, indice_gini_industria, indice_gini_servicos, indice_gini_administracao)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for idx, i in df_gini.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['data'],
                i['Índice de Gini da distribuição do produto interno bruto a preços correntes'],
                i['unidade'],
                i['Índice de Gini da distribuição do valor adicionado bruto a preços correntes da agropecuária'],
                i['Índice de Gini da distribuição do valor adicionado bruto a preços correntes da indústria'],
                i['Índice de Gini da distribuição do valor adicionado bruto a preços correntes dos serviços, exclusive administração, defesa, educação e saúde públicas e seguridade social'],
                i['Índice de Gini da distribuição do valor adicionado bruto a preços correntes da administração, defesa, educação e saúde públicas e seguridade social']
            )
            cur.execute(inserindo_indice_gini, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados: {e}")
        # conexao.rollback()  

    conexao.commit()
    conexao.close()