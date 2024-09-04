from dagster import asset
import pandas as pd
from .constants import *
from pyspark.sql.functions import col,regexp_replace,lit

@asset(deps=["bancos_trusted"])
def empregados_trusted() -> None:

    df_bancos = pd.read_parquet(f'{TRUSTED_FOLDER}/Bancos/bancos.parquet')
    df_empregados1 = pd.read_csv(f'{RAW_FOLDER}/Empregados/glassdoor_consolidado_join_match_less_v2.csv', sep='|', encoding='utf-8',dtype={'CNPJ':str})
    df_empregados2 = pd.read_csv(f'{RAW_FOLDER}/Empregados/glassdoor_consolidado_join_match_v2.csv', sep='|', encoding='utf-8',dtype={'CNPJ':str})
    
    df_empregados1 = df_empregados1.merge(right=df_bancos, how='left',on='CNPJ')
    df_empregados1 = df_empregados1.drop(columns=['Nome_y'])
    df_empregados1 = df_empregados1.rename(columns={'Nome_x' : 'Nome'})


    df_empregados2 = df_empregados2.merge(right=df_bancos, how='left',on='Nome')
    df_empregados2 = df_empregados2.drop(columns=['Segmento_y'])
    df_empregados2 = df_empregados2.rename(columns={'Segmento_x' : 'Segmento'})

    df_empregados=pd.concat([df_empregados1,df_empregados2])
    df_empregados.drop_duplicates(inplace=True)
    
    df_empregados = df_empregados.rename(columns={
            "employer-website":"employer_website",
            "employer-headquarters":"employer_headquarters",
            "employer-founded":"employer_founded",
            "employer-industry":"employer_industry",
            "employer-revenue":"employer_revenue",
            "Geral":"geral",
            "Cultura e valores":"cultura_e_valores",
            "Diversidade e inclusão":"diversidade_e_inclusao",
            "Qualidade de vida":"qualidade_de_vida",
            "Alta liderança":"alta_lideranca",
            "Remuneração e benefícios":"remuneracao_e_beneficios",
            "Oportunidades de carreira":"oportunidades_de_carreira",
            "Recomendam para outras pessoas(%)":"recomendam_para_outras_pessoas",
            "Perspectiva positiva da empresa(%)":"perspectiva_positiva_da_empresa",
            "CNPJ":"cnpj",
            "Nome":"nome",
            "Segmento":"segmento"
        })
    
    df_empregados.to_parquet(f'{TRUSTED_FOLDER}/Empregados/empregados.parquet')