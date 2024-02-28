import os
from pathlib import Path
import pandas as pd
import pandera as pa
from dotenv import dotenv_values
from sqlalchemy import create_engine
from schema import ProductSchema, ProductSchemaKPI
import duckdb
from sqlalchemy.sql import text

def load_settings():
    """Load settings from .env file"""
    env_path = Path.cwd() / '.env'
    #load_dotenv(dotenv_path=env_path)
    env = dotenv_values(env_path, encoding="utf-8")
    settings = {
        "db_host": env.get("POSTGREST_HOST"),
        "db_user": env.get("POSTGREST_USER"),
        "db_pass": env.get("POSTGREST_PASSWORD"),
        "db_port": env.get("POSTGREST_PORT"),
        "db_name": env.get("POSTGREST_DATABASE")
    }

    return settings

@pa.check_output(ProductSchema, lazy=True)
def extract(query: str) -> pd.DataFrame:
    """
    Extrai dados do banco de dados SQL usando a consulta fornecida.

    Args:
        query: A consulta SQL para extrair dados.

    Returns:
        Um DataFrame do Pandas contendo os dados extraídos.
    """
    settings = load_settings()
    connection_string = f"postgresql://{settings['db_user']}:{settings['db_pass']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}"
    engine = create_engine(connection_string)
    
    with engine.connect() as conn, conn.begin():
        df_crm = pd.read_sql(text(query), conn)
    
    return df_crm

@pa.check_input(ProductSchema, lazy=True)
@pa.check_output(ProductSchemaKPI, lazy=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma os dados do DataFrame aplicando cálculos e normalizações.

    Args:
        df: DataFrame do Pandas contendo os dados originais.

    Returns:
        DataFrame do Pandas após a aplicação das transformações.
    """
    # Calcular valor_total_estoque
    df['valor_total_estoque'] = df['quantidade'] * df['preco']
    
    # Normalizar categoria para maiúsculas
    df['categoria_normalizada'] = df['categoria'].str.lower()
    
    # Determinar disponibilidade (True se quantidade > 0)
    df['disponibilidade'] = df['quantidade'] > 0

    return df

@pa.check_input(ProductSchemaKPI, lazy=True)
def load(df: pd.DataFrame, table_name: str, db_file: str = 'my_duckdb.db'):
    """
    Carrega o DataFrame no DuckDB, criando ou substituindo a tabela especificada.

    Args:
        df: DataFrame do Pandas para ser carregado no DuckDB.
        table_name: Nome da tabela no DuckDB onde os dados serão inseridos.
        db_file: Caminho para o arquivo DuckDB. Se não existir, será criado.
    """
    con = duckdb.connect(database=db_file, read_only=False)
    
    con.register('df_temp', df)
    
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_temp")
    
    con.close()


if __name__ == "__main__":
    
    query = "SELECT * FROM produtos_bronze_email"
    df_crm = extract(query=query)
    df_crm_kpi = transform(df_crm)

    print(df_crm)
    schema_crm = pa.infer_schema(df_crm)
    with open("infered_schema.py", "w", encoding="utf-8") as arquivo:
         arquivo.write(schema_crm.to_script())

    print(df_crm_kpi)
    schema_crm = pa.infer_schema(df_crm_kpi)
    with open("infered_schema_kpi.py", "w", encoding="utf-8") as arquivo:
         arquivo.write(schema_crm.to_script())

    # with open("inferred_schema.json", "r") as file:
    #      file.write(df_crm_kpi.to_json())


    load(df=df_crm_kpi, table_name="tabela_kpi")