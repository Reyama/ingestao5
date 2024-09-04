from dagster import asset
import pandas as pd
from .constants import *
from pyspark.sql.functions import col,regexp_replace,lit


@asset
def bancos_trusted() -> None:
    df_bancos = pd.read_csv(f'{RAW_FOLDER}/Bancos/EnquadramentoInicia_v2.tsv',sep="\t",encoding="iso-8859-1",dtype={'CNPJ':str})
    df_bancos['Nome'] = df_bancos['Nome'].str.replace(' - PRUDENCIAL','')
    df_bancos.to_parquet(f"{TRUSTED_FOLDER}/Bancos/bancos.parquet")

