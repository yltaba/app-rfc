import streamlit as st
import pandas as pd
from io import BytesIO

st.title('Caldic LATAM - File Consolidator')

with st.expander("Read more:"):
    st.write("""
             This application consolidates Excel files that have the same structure, matching all columns and their headers sequentially.
             
             The output is a single Excel file available on the "Download Final Excel" button below.

             **All files must be in the same template.**
            """)

uploaded_files = st.file_uploader("Upload RFC's Excel files", accept_multiple_files=True)

def process_files(uploaded_files):
    df_list = []
    for file in uploaded_files:
        if file:
            df = pd.read_excel(file)
            df_list.append(df)
    final_df = pd.concat(df_list, ignore_index=True)
    return final_df

if uploaded_files:
    final_df = process_files(uploaded_files)
    st.write("Sample:")
    st.dataframe(final_df)

    towrite = BytesIO()
    final_df.to_excel(towrite, index=False)
    towrite.seek(0) 

    st.download_button(label="📥 Download Final Excel",
                       data=towrite,
                       file_name='rfc_complete.xlsx',
                       mime="application/vnd.ms-excel")

                       