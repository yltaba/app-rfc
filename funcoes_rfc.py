import pandas as pd
import unidecode
import numpy as np
import janitor

mes0 = '12-2023'
mes1 = '11-2023'
mes2 = '10-2023'
mes3 = '9-2023'
mes4 = '8-2023'
mes5 = '7-2023'
mes6 = '6-2023'
mes7 = '5-2023'
mes8 = '4-2023'
mes9 = '3-2023'
mes10 = '2-2023'
mes11 = '1-2023'
mes12 = '1-2023'

l3m = [mes1, mes2, mes3]
l6m = [mes1, mes2, mes3, mes4, mes5, mes6]
l12m = [mes1, mes2, mes3, mes4, mes5, mes6, mes7, mes8, mes9, mes10, mes11, mes12]

# PPC
def ponderar_ppc(ppc):

    ppc.columns = (
        ppc.columns
        .str.replace('\\$', '', regex=True)
        .str.replace('\\(', '', regex=True)
        .str.replace('\\)', '', regex=True)
        .str.replace('\\/', ' ', regex=True)
        )
    ppc = janitor.clean_names(ppc)
    ppc = ppc[[
        'country', 'warehouse', 'product_code', 
        'product_desc', 'stock_ton', 'ppc_sin_almacenaje'
    ]].copy()
    ppc['country'] = [unidecode.unidecode(string) for string in ppc['country']]

    ppc['ppc_sin_almacenaje'] = ppc['ppc_sin_almacenaje'] / 1000
    ppc['total_stock_ton'] = ppc.groupby(['country', 'product_code'], as_index=False).stock_ton.transform('sum')
    ppc['pesos'] = ppc['stock_ton'] / ppc['total_stock_ton']
    ppc['ppc_ponderado'] = ppc['ppc_sin_almacenaje'] * ppc['pesos']

    ppc = (
        ppc
        .groupby(['country', 'product_code'], as_index=False)
        .agg(custo_ppc=('ppc_ponderado','sum'))
    )
    ppc['custo_ppc'] = (ppc['custo_ppc']) * 1.07

    # 7% = 3% storage e 4% outbound freight
    # adicionado somente para PPC, custo histórico já contempla essas informações.
    
    ppc = ppc.rename(columns={
        'country':'from_country',
        'product_code':'cod_product'
        })
    
    custo_ppc = (
         ppc[['from_country', 'cod_product', 'custo_ppc']]
        .drop_duplicates(subset=['from_country', 'cod_product'], keep='first')
        )

    return custo_ppc

# BASE VENDAS
def tratar_base_vendas(test_comercial, de_para_bu):

    test_comercial = test_comercial.rename(
        columns={
            'Fecha':'fecha',
            "Pais'_2":'pais_compania', 
            'Mes':'mes',
            'Cod Articulo':'codigo_producto',
            'BU_Adj_View':'bu',
            'Cat_Est_Adj2':'segmento_producto',
            'Client_Adj':'nombre_cliente',
            'Articulo Std':'product_description',
            'Cod Cliente':'codigo_cliente',
            'Segmento_Ajustado':'segmento_cliente',
            'Volume Actual 2023':'total_toneladas_vendidas', 
            'Venta Actual 2023':'total_monto_venta_usd',
            'Margem Actual 2023':'total_margen_usd'
            }
            )

    # filtro de ajustes
    test_comercial = test_comercial[test_comercial.codigo_producto != 'Ajuste']

    # de para BU nova estrutura
    
    test_comercial = pd.merge(test_comercial, de_para_bu, on='segmento_cliente', how='left')
    test_comercial['bu_nova'] = test_comercial['bu_nova'].fillna('NA')
    test_comercial['bu'] = np.where(test_comercial['bu_nova'] == 'NA', test_comercial['bu'], test_comercial['bu_nova'])
    test_comercial = test_comercial.drop('bu_nova', axis=1)

    # tirando '000
    test_comercial['total_margen_usd'] = test_comercial['total_margen_usd'] * 1000
    test_comercial['total_monto_venta_usd'] = test_comercial['total_monto_venta_usd'] * 1000

    # ajustes pais
    test_comercial['pais_compania'] = test_comercial.pais_compania.str.upper()
    test_comercial['pais_compania'] = [unidecode.unidecode(string) for string in test_comercial['pais_compania']]

    # datas
    test_comercial['fecha'] = pd.to_datetime(test_comercial.fecha, dayfirst=True)
    test_comercial['ano'] = pd.to_datetime(test_comercial.fecha).dt.year
    test_comercial['mes_str'] = test_comercial.mes.astype(str)
    test_comercial['ano_str'] = test_comercial.ano.astype(str)
    test_comercial['mes_ano'] = test_comercial.mes_str + '-' + test_comercial.ano_str
    
    test_comercial['codigo_producto'] = test_comercial['codigo_producto'].str.upper()
    
    test_comercial['segmento_producto'] = test_comercial.segmento_producto.str.upper()
    test_comercial['segmento_producto'] = test_comercial.segmento_producto.replace('INDUSTRIALS', 'ESSENTIALS')
    test_comercial['segmento_producto'] = test_comercial.segmento_producto.replace('INDUSTRIAL', 'ESSENTIALS')
    
    test_comercial['nombre_cliente'] = test_comercial.nombre_cliente.fillna('')
    
    test_comercial = test_comercial[~test_comercial.nombre_cliente.str.contains('GTM')]
    test_comercial_l12m = test_comercial[test_comercial.mes_ano.isin([
        mes1, mes2, mes3, mes4, mes5, mes6, 
        mes7, mes8, mes9, mes10, mes11, mes12
        ])]

    return test_comercial, test_comercial_l12m

# CUSTOS BASE VENDAS
def calcular_custo_base_vendas_l3m(df_vendas_l3m):

    custo_base_vendas = (
        df_vendas_l3m
        .groupby(['mes_ano', 'pais_compania', 'codigo_producto'], as_index=False) # fecha, nombre_cliente
        .agg({
            'total_monto_venta_usd':'sum',
            'total_margen_usd':'sum',
            'total_toneladas_vendidas':'sum'
        })
    )

    custo_base_vendas = custo_base_vendas.rename(columns={
        'codigo_producto':'cod_product',
        'pais_compania':'from_country'
    })

    custo_base_vendas['custo_total_base_vendas'] = custo_base_vendas.total_monto_venta_usd - custo_base_vendas.total_margen_usd
    custo_base_vendas['custo_total_base_vendas'] = custo_base_vendas['custo_total_base_vendas'] / 1000
    custo_base_vendas['custo_unitario_base_vendas'] = custo_base_vendas.custo_total_base_vendas / custo_base_vendas.total_toneladas_vendidas

    custo_base_vendas = custo_base_vendas.replace([np.inf, -np.inf], np.nan)

    custo_base_vendas['custo_unitario_base_vendas'] = np.where(
        (custo_base_vendas.total_margen_usd < 0) | ((custo_base_vendas.total_toneladas_vendidas <= 0) & (custo_base_vendas.total_monto_venta_usd <= 0)),
        0,
        custo_base_vendas.custo_unitario_base_vendas
    )

    custo_base_vendas['custo_unitario_base_vendas'] = custo_base_vendas['custo_unitario_base_vendas'].replace([np.inf, -np.inf], np.nan).fillna(0)

    custo_base_vendas = custo_base_vendas.pivot(index=[
        'from_country', 'cod_product'
        ], columns='mes_ano', values='custo_unitario_base_vendas').reset_index()


    custo_base_vendas = custo_base_vendas.rename(columns={
        mes1:mes1+'_custo_base_vendas',
        mes2:mes2+'_custo_base_vendas',
        mes3:mes3+'_custo_base_vendas',
        mes4:mes4+'_custo_base_vendas',
        mes5:mes5+'_custo_base_vendas',
        mes6:mes6+'_custo_base_vendas',
        mes7:mes7+'_custo_base_vendas',
        mes8:mes8+'_custo_base_vendas',
        mes9:mes9+'_custo_base_vendas',
        mes10:mes10+'_custo_base_vendas',
        mes11:mes11+'_custo_base_vendas',
        mes12:mes12+'_custo_base_vendas'
    })

    custo_base_vendas = custo_base_vendas.fillna(0)

    return custo_base_vendas

# MARGENS BASE VENDAS
def calcular_margem_l3m(df_vendas):

    vendas_l3m = df_vendas.loc[df_vendas['mes_ano'].isin(l3m)].copy()
    vendas_l3m = vendas_l3m.rename(columns={
        'pais_compania':'from_country',
        'codigo_producto':'cod_product'
    })
    
    vendas_l3m_agrupado = (
        vendas_l3m
        .groupby(['from_country', 'bu', 'cod_product'], as_index=False)
        .agg({
            'total_monto_venta_usd': 'sum',
            'total_margen_usd': 'sum',
            'total_toneladas_vendidas': 'sum'
        })
    )

    vendas_l3m_agrupado['margem_l3m'] = vendas_l3m_agrupado['total_margen_usd'] / vendas_l3m_agrupado['total_monto_venta_usd']
    vendas_l3m_agrupado['margem_l3m'] = vendas_l3m_agrupado['margem_l3m'].replace([-np.inf, np.inf], 0)
    vendas_l3m_agrupado['margem_l3m'] = vendas_l3m_agrupado.apply(
        lambda row: 0 if row['total_toneladas_vendidas'] < 0.001 else row['margem_l3m'], axis=1
    )

    vendas_l3m_agrupado = vendas_l3m_agrupado.drop(['total_monto_venta_usd', 'total_margen_usd', 'total_toneladas_vendidas'], axis=1)

    return vendas_l3m_agrupado

def calcular_margem_l6m(df_vendas):

    vendas_l6m = df_vendas.loc[df_vendas['mes_ano'].isin(l6m)].copy()

    vendas_l6m = vendas_l6m.rename(columns={
        'pais_compania':'from_country',
        'codigo_producto':'cod_product'
    })
    
    vendas_l6m_agrupado = (
        vendas_l6m
        .groupby(['from_country', 'bu', 'cod_product'], as_index=False)
        .agg({
            'total_monto_venta_usd': 'sum',
            'total_margen_usd': 'sum',
            'total_toneladas_vendidas': 'sum'
        })
    )

    vendas_l6m_agrupado['margem_l6m'] = vendas_l6m_agrupado['total_margen_usd'] / vendas_l6m_agrupado['total_monto_venta_usd']
    vendas_l6m_agrupado['margem_l6m'] = vendas_l6m_agrupado['margem_l6m'].replace([-np.inf, np.inf], 0)
    vendas_l6m_agrupado['margem_l6m'] = vendas_l6m_agrupado.apply(
        lambda row: 0 if row['total_toneladas_vendidas'] < 0.001 else row['margem_l6m'], axis=1
    )

    vendas_l6m_agrupado = vendas_l6m_agrupado.drop(['total_monto_venta_usd', 'total_margen_usd', 'total_toneladas_vendidas'], axis=1)

    return vendas_l6m_agrupado

def calcular_margem_l12m(df_vendas):

    vendas_l12m = df_vendas.loc[df_vendas['mes_ano'].isin(l12m)].copy()

    vendas_l12m = vendas_l12m.rename(columns={
        'pais_compania':'from_country',
        'codigo_producto':'cod_product'
    })
    
    vendas_l12m_agrupado = (
        vendas_l12m
        .groupby(['from_country', 'bu', 'cod_product'], as_index=False)
        .agg({
            'total_monto_venta_usd': 'sum',
            'total_margen_usd': 'sum',
            'total_toneladas_vendidas': 'sum'
        })
    )

    vendas_l12m_agrupado['margem_l12m'] = vendas_l12m_agrupado['total_margen_usd'] / vendas_l12m_agrupado['total_monto_venta_usd']
    vendas_l12m_agrupado['margem_l12m'] = vendas_l12m_agrupado['margem_l12m'].replace([-np.inf, np.inf], 0)
    vendas_l12m_agrupado['margem_l12m'] = vendas_l12m_agrupado.apply(
        lambda row: 0 if row['total_toneladas_vendidas'] < 0.001 else row['margem_l12m'], axis=1
    )

    vendas_l12m_agrupado = vendas_l12m_agrupado.drop(['total_monto_venta_usd', 'total_margen_usd', 'total_toneladas_vendidas'], axis=1)

    return vendas_l12m_agrupado

# VALORAÇÃO RFC
def importar_tratar_rfc(rfc, de_para_bu):

    rfc = rfc.rename(columns=lambda x: x.replace('\n', ''))
    rfc = rfc.rename(columns=lambda x: x.replace(')', ''))

    rfc = janitor.clean_names(rfc)

    rename_cols_rfc = {
        'cod_product_sku':'cod_product',
        'cod_client':'codigo_cliente',
        'business_unit':'bu'
    }
    rfc = rfc.rename(columns=rename_cols_rfc)

    replace_bu = {
        'O&G Y MINERÍA':'OIL & GAS',
        'EXTRACTIVE INDUSTRIES':'OIL & GAS',
        'LIFE SCIENCE':'LIFESCIENCE',
        'LIFE SCIENCES':'LIFESCIENCE',
        'FPP (Flexible printing packaging)':'FLEXIBLE PRINT & PACKAGING',
        'FPP':'FLEXIBLE PRINT & PACKAGING',
        'ESPECIALIDADES':'SPECIALTIES',
        'INDUSTRIAL':'INDUSTRIALS',
        'Industrial':'INDUSTRIALS',
        'MINING':'MINERÍA'
    }

    rfc['bu'] = rfc['bu'].replace(replace_bu)

    # nova estrutura BU
    de_para_bu = de_para_bu.rename(columns={'segmento_cliente':'customer_segment'})
    rfc = pd.merge(rfc, de_para_bu, on='customer_segment', how='left')
    rfc['bu_nova'] = rfc['bu_nova'].fillna('NA')
    rfc['bu'] = np.where(rfc['bu_nova'] == 'NA', rfc['bu'], rfc['bu_nova'])
    rfc = rfc.drop('bu_nova', axis=1)

    txt_cols_rfc = ['from_country', 'cod_product', 'codigo_cliente', 'bu']
    for col in txt_cols_rfc:
        rfc[col] = rfc[col].astype(str).str.strip().str.upper()

    # rfc['margem_rfc_aprovado'] = rfc['gp_rfc_aprovado'] / rfc['net_sales_usd_rfc_aprovado']

    return rfc

def incluir_custo_preco_base_rfc(df_rfc, df_custo_base_vendas, df_custo_ppc, df_margem_base_vendas):

    # custos
    rfc_custo_vendas = pd.merge(df_rfc, df_custo_base_vendas, on=['from_country', 'cod_product'], how='left')
    rfc_custo = pd.merge(rfc_custo_vendas, df_custo_ppc, on=['from_country', 'cod_product'], how='left')

    # margens
    rfc_custo_margem = pd.merge(rfc_custo, df_margem_base_vendas, on=['from_country', 'bu', 'cod_product'], how='left')
    
    return rfc_custo_margem

def definir_custo(df_rfc_custo_preco):

    num_cols = [
        mes1 + '_custo_base_vendas', mes2 + '_custo_base_vendas', mes3 + '_custo_base_vendas', mes4 + '_custo_base_vendas',
        mes5 + '_custo_base_vendas', mes6 + '_custo_base_vendas', mes7 + '_custo_base_vendas', mes8 + '_custo_base_vendas',
        mes9 + '_custo_base_vendas', mes10 + '_custo_base_vendas', mes11 + '_custo_base_vendas', mes12 + '_custo_base_vendas',
        'custo_ppc'
        ]
    for col in num_cols:
        df_rfc_custo_preco[col] = df_rfc_custo_preco[col].astype(float).fillna(0)

    custo_final = []
    criterio_custo = []
    for _, row in df_rfc_custo_preco.iterrows():
        
        if (row['custo_ppc'] > 0):
            custo_final.append(row['custo_ppc'])
            criterio_custo.append('PPC')

        elif (row[mes1 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes1 + '_custo_base_vendas'])
            criterio_custo.append(mes1)

        elif (row[mes2 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes2 + '_custo_base_vendas'])
            criterio_custo.append(mes2)

        elif (row[mes3 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes3 + '_custo_base_vendas'])
            criterio_custo.append(mes3)

        elif (row[mes4 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes4 + '_custo_base_vendas'])
            criterio_custo.append(mes4)

        elif (row[mes5 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes5 + '_custo_base_vendas'])
            criterio_custo.append(mes5)

        elif (row[mes6 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes6 + '_custo_base_vendas'])
            criterio_custo.append(mes6)

        elif (row[mes7 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes7 + '_custo_base_vendas'])
            criterio_custo.append(mes7)

        elif (row[mes8 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes8 + '_custo_base_vendas'])
            criterio_custo.append(mes8)

        elif (row[mes9 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes9 + '_custo_base_vendas'])
            criterio_custo.append(mes9)

        elif (row[mes10 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes10 + '_custo_base_vendas'])
            criterio_custo.append(mes10)

        elif (row[mes11 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes11 + '_custo_base_vendas'])
            criterio_custo.append(mes11)

        elif (row[mes12 + '_custo_base_vendas'] > 0):
            custo_final.append(row[mes12 + '_custo_base_vendas'])
            criterio_custo.append(mes12)

        else:
            custo_final.append(0)
            criterio_custo.append('NA')

    df_rfc_custo_preco['custo_final'] = custo_final
    df_rfc_custo_preco['criterio_custo'] = criterio_custo

    df_rfc_custo_preco = df_rfc_custo_preco.drop([
        mes1 + '_custo_base_vendas', mes2 + '_custo_base_vendas', mes3 + '_custo_base_vendas', mes4 + '_custo_base_vendas',
        mes5 + '_custo_base_vendas', mes6 + '_custo_base_vendas', mes7 + '_custo_base_vendas', mes8 + '_custo_base_vendas',
        mes9 + '_custo_base_vendas', mes10 + '_custo_base_vendas', mes11 + '_custo_base_vendas', mes12 + '_custo_base_vendas',
        'custo_ppc'
        ], axis=1)

    return df_rfc_custo_preco

def definir_margem(df):
    margem_cols = [
       'margem_l12m', 'margem_l6m', 'margem_l3m'
    ]
    df[margem_cols] = df[margem_cols].fillna(0)

    # Definição do preço a ser utilizado na valoração
    margem_final = []
    criterio_margem = []

    for _, row in df.iterrows():

        if row['margem_l3m'] != 0 and row['margem_l3m'] < 0.9:
            margem_final.append(row['margem_l3m'])
            criterio_margem.append('L3M')

        elif row['margem_l6m'] != 0 and row['margem_l6m'] < 0.9:
            margem_final.append(row['margem_l6m'])
            criterio_margem.append('L6M')

        elif row['margem_l12m'] != 0 and row['margem_l12m'] < 0.9:
            margem_final.append(row['margem_l12m'])
            criterio_margem.append('L12M')

        else:
            margem_final.append(0)
            criterio_margem.append('NA')

    df['margem_final'] = margem_final
    df['criterio_margem'] = criterio_margem

    df = df.drop([
        'margem_l12m', 'margem_l6m', 'margem_l3m'
        ], axis=1)
    
    return df

def calcular_custo_receita_gp(df):

    df['gross_up'] = df['custo_final'] / (1 - df['margem_final'])

    df['receita'] = df['gross_up'] * df['volume_ton']

    df['custo_total'] = df['custo_final'] * df['volume_ton']

    df['gross_profit'] = df['receita'] - df['custo_total']

    return df