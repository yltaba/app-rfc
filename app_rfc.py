import pandas as pd
import streamlit as st
from io import BytesIO
from openpyxl import load_workbook

import funcoes_rfc


hoje = '15012024'
arquivo_output = 'rfc_02_lan'


# apresentação app
st.title("RFC LAN - Volume Valuation Model")
st.subheader("Caldic LATAM - Business Analytics")

with st.expander("Read more:"):
    st.markdown(
        """
        This app automates the RFC's volume valuation process for countries in LAN.

        *Instructions:*

            1. Upload the RFC file with the volumes. 
            This file must have only one tab and must begin at line 1.
            It also must to fit with RFC Sales Plan template.

            2. Wait for file upload.

            3. When uploaded, a button called "Run valuation" will pop up. 
            Press it and wait.

            4. Output table with valuated volumes will be available to download.
        """
    )

# parâmetros e bases
MES0 = '1-2024'
MES1 = '12-2023'
MES2 = '11-2023'
MES3 = '10-2023'
MES4 = '9-2023'
MES5 = '8-2023'
MES6 = '7-2023'
MES7 = '6-2023'
MES8 = '5-2023'
MES9 = '4-2023'
MES10 = '3-2023'
MES11 = '2-2023'
MES12 = '1-2023'

l3m = [MES1, MES2, MES3]
l6m = [MES1, MES2, MES3, MES4, MES5, MES6]
l12m = [MES1, MES2, MES3, MES4, MES5, MES6, MES7, MES8, MES9, MES10, MES11, MES12]


# Imports
PATH = r'C:/Users/y.lucatelli.CALDICCGN/Caldic Global/Business Analytics - Documentos/Python_Consultas/projetos_python/01_RFC/LAN/2024/RFC 1/bases/'
BASE_PPC = 'Adherence Details by Product.xlsx'
BASE_VENDAS = 'vendas_commercial_performance.xlsx'

test_comercial = pd.read_excel(BASE_VENDAS, sheet_name='base_modelo')
ppc = pd.read_excel(BASE_PPC)
rfc_uploaded = st.file_uploader("Insira o arquivo com volumes:" )
de_para_bu = pd.read_excel('DexPara nueva estructura.xlsx')


# valorização
if rfc_uploaded and st.button('Run valuation'):
    with st.spinner('Running valuation...'):
        rfc = pd.read_excel(rfc_uploaded)

        # custos
        custo_ppc = funcoes_rfc.ponderar_ppc(ppc)
        test_comercial, test_comercial_l12m = funcoes_rfc.tratar_base_vendas(test_comercial=test_comercial, 
                                                                             de_para_bu=de_para_bu)
        custo_base_vendas = funcoes_rfc.calcular_custo_base_vendas_l3m(test_comercial_l12m)

        # margens
        margem_l3m = funcoes_rfc.calcular_margem_l3m(test_comercial_l12m)
        margem_l6m = funcoes_rfc.calcular_margem_l6m(test_comercial_l12m)
        margem_l12m = funcoes_rfc.calcular_margem_l12m(test_comercial_l12m)
        margem_temp1 = pd.merge(margem_l12m, 
                                margem_l6m, 
                                on=['from_country', 'bu', 'cod_product'], 
                                how='left')
        margem_base_vendas = pd.merge(margem_temp1, 
                                      margem_l3m, 
                                      on=['from_country', 'bu', 'cod_product'], 
                                      how='left')
        for col in margem_base_vendas.columns:
            if 'margem' in col:
                margem_base_vendas[col] = margem_base_vendas[col].fillna(0)

        # valoração RFC
        rfc = funcoes_rfc.importar_tratar_rfc(rfc, de_para_bu)
        rfc_custo_margem = funcoes_rfc.incluir_custo_preco_base_rfc(rfc, 
                                                                    custo_base_vendas, 
                                                                    custo_ppc, 
                                                                    margem_base_vendas)
        rfc_custo_margem = funcoes_rfc.definir_custo(rfc_custo_margem)
        rfc_custo_margem = funcoes_rfc.definir_margem(rfc_custo_margem)
        rfc_custo_margem = funcoes_rfc.calcular_custo_receita_gp(rfc_custo_margem)

        # tratamentos finais RFC
        MODELO = '_modelo'
        rfc_custo_margem = rfc_custo_margem.rename(columns={
            'custo_final':'custo_final'+MODELO,
            'criterio_custo':'criterio_custo'+MODELO,
            'margem_final':'margem_final'+MODELO,
            'criterio_margem':'criterio_margem'+MODELO,
            'gross_up':'gross_up'+MODELO,
            'receita':'receita'+MODELO,
            'custo_total':'custo_total'+MODELO,
            'gross_profit':'gross_profit'+MODELO
        })


        excel_ajustado = funcoes_rfc.ajustar_excel(rfc_custo_margem, arquivo_output, hoje)


        # EXPORT EXCEL
        towrite = BytesIO()
        with open(excel_ajustado, 'rb') as f:
            towrite.write(f.read())
        towrite.seek(0)

        st.download_button(label="📥 Download Valuated RFC",
                        data=towrite,
                        file_name='rfc_valuation_model.xlsx',
                        mime="application/vnd.ms-excel")

        # resultados

        rfc_custo_margem['month'] = pd.to_datetime(rfc_custo_margem['month'], dayfirst=True)

        st.success("Volume valuation done!")
        st.dataframe(
            rfc_custo_margem
            .loc[rfc_custo_margem['month'] == '2024-02-01']
            .groupby(['from_country', 'month'], as_index=False)
            .agg({
                'volume_ton':'sum',
                'gross_profit'+MODELO:'sum'
            })
            .sort_values(['gross_profit'+MODELO], ascending=False)
            .round(2)
            .reset_index(drop=True)
        )
