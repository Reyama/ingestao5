from dagster import asset
import pandas as pd
from .constants import *
from pyspark.sql.functions import col,regexp_replace,lit
import os
from ..resources.database import SQLAlchemyResource


@asset
def reclamacoes_trusted() -> None:
    all_files = os.listdir(f'{RAW_FOLDER}/Reclamações/')

    lista_dataframe=[]
    for i, filename in enumerate(all_files):
        try:
            df = pd.read_csv(f'{RAW_FOLDER}/Reclamações/{filename}', sep=';', encoding='iso-8859-1')
            lista_dataframe.append(df)
        except:
            print('Não foi possível abrir o arquivo')
            pass
    
    df_reclamacoes = pd.concat(lista_dataframe)
    df_reclamacoes = df_reclamacoes.rename(columns={
            "Ano":"ano",
            "Trimestre":"trimestre",
            "Tipo":"tipo",
            "CNPJ IF":"cnpj_if",
            "Instituição financeira":"instituicao_financeira",
            "Índice":"indice",
            "Quantidade de reclamações reguladas procedentes":"quantidade_de_reclamacoes_reguladas_procedentes",
            "Quantidade de reclamações reguladas - outras":"quantidade_de_reclamacoes_reguladas_outras",
            "Quantidade de reclamações não reguladas":"quantidade_de_reclamacoes_nao_reguladas",
            "Quantidade total de reclamações":"quantidade_total_de_reclamacoes",
            "Quantidade total de clientes  CCS e SCR":"quantidade_total_de_clientes_ccs_e_scr",
            "Quantidade de clientes  CCS":"quantidade_de_clientes_ccs",
            "Quantidade de clientes  SCR":"quantidade_de_clientes_scr"
        })
    df_reclamacoes = df_reclamacoes.drop(columns=['Unnamed: 14'])
    df_reclamacoes["quantidade_total_de_clientes_ccs_e_scr"] = pd.to_numeric(df_reclamacoes["quantidade_total_de_clientes_ccs_e_scr"], errors="coerce")
    df_reclamacoes["quantidade_de_clientes_ccs"] = pd.to_numeric(df_reclamacoes["quantidade_de_clientes_ccs"], errors="coerce")
    df_reclamacoes["quantidade_de_clientes_scr"] = pd.to_numeric(df_reclamacoes["quantidade_de_clientes_scr"], errors="coerce")
    df_reclamacoes.to_parquet(f'{TRUSTED_FOLDER}/Reclamações/reclamacoes.parquet')
    

@asset(deps=['reclamacoes_trusted', 'empregados_trusted'])
def reclamacoes_delivery(db_connection: SQLAlchemyResource) -> None:
    engine = db_connection.get_engine()
    
    df_empregados = pd.read_parquet(f'{TRUSTED_FOLDER}/Empregados/empregados.parquet')
    df_reclamacoes = pd.read_parquet(f'{TRUSTED_FOLDER}/Reclamações/reclamacoes.parquet')
    df_out = df_reclamacoes.merge(right=df_empregados, how='inner',left_on='cnpj_if',right_on='cnpj' )
    
    try:
        df_out.to_sql('reclamacoes_delivery', engine, index=False, if_exists='append')
    except Exception as error:
        print(f"Error: {error}")