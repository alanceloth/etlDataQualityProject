import os
import io
import locale
from pathlib import Path
from dotenv import load_dotenv, dotenv_values
from sqlalchemy import create_engine, MetaData
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

def tabela_existe(engine, nome_tabela):
    """Check if table exists in the database"""
    meta = MetaData()
    meta.reflect(bind=engine)
    return nome_tabela in meta.tables

def executa_query(engine, query: str):
    """Execute a query on the database"""
    with engine.connect() as conn, conn.begin():
        conn.execute(text(query))

def guess_encoding(file):
    """Guess the encoding of the given file"""
    with io.open(file, "rb") as f:
        data = f.read(5)

    if data.startswith(b"\xEF\xBB\xBF"):  # UTF-8 with BOM
        return "utf-8-sig"
    elif data.startswith(b"\xFF\xFE") or data.startswith(b"\xFE\xFF"):
        return "utf-16"
    else:
        try:
            with io.open(file, encoding="utf-8") as f:
                f.read()
            return "utf-8"
        except UnicodeDecodeError:
            print(locale.getdefaultlocale()[1])
            return locale.getdefaultlocale()[1]

def get_table_name_from_create_query(query: str) -> str:
    """
    Extracts the table name from a CREATE TABLE query.
    Assumes that the query follows the standard format:
    CREATE TABLE <table_name> ...
    """
    # Split the query into lines
    lines = query.strip().split('\n')

    # Get the first line and split it by whitespace
    first_line_tokens = lines[0].strip().split()

    # The table name is the third token
    table_name = first_line_tokens[2]

    return table_name

def main():
    # Load database settings from .env file
    settings = load_settings()

    # Create database connection string
    connection_string = f"postgresql://{settings['db_user']}:{settings['db_pass']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}"
    
    try:
        # Create engine
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect():
            print("Connection successful!")
    except Exception as e:
        print("Error establishing connection:", e)

    # List of SQL files to be executed
    arquivos_sql = [
        'create_table_produtos_bronze_email.sql',
        'insert_into_tabela_bronze_email.sql',
        'create_table_produtos_bronze.sql',
        'insert_into_tabela_bronze.sql'
    ]

    # Execute queries
    for arquivo in arquivos_sql:
        # Get full path of SQL file
        sql_file_path = os.path.join('sql', arquivo)

        # Read SQL file content with guessed encoding
        with open(sql_file_path, 'r', encoding=guess_encoding(sql_file_path)) as file:
            sql_query = str.replace(file.read(), '\n', '')
        
        # Get table name
        nome_tabela = get_table_name_from_create_query(sql_query)

        # Check if table exists
        if not tabela_existe(engine, nome_tabela):
            # Execute query
            executa_query(engine, sql_query)
            print(f"Tabela {nome_tabela} criada e/ou populada com sucesso.")
        else:
            print(f"A tabela {nome_tabela} já existe no banco de dados.")

if __name__ == "__main__":
    main()
