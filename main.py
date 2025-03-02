import json
import pandas as pd
import pyodbc
import os

def load_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def get_db_columns(cursor, table_name):
    # Ajustado para lidar corretamente com o nome da tabela com esquema
    cursor.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name.split('.')[-1]}' AND TABLE_SCHEMA = '{table_name.split('.')[0]}'
    """)
    return [row[0] for row in cursor.fetchall()]

def get_db_column_types(cursor, table_name):
    # Consultar os tipos de dados das colunas da tabela
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name.split('.')[-1]}' 
        AND TABLE_SCHEMA = '{table_name.split('.')[0]}'
    """)
    return {row[0]: row[1] for row in cursor.fetchall()}

def load_data(base_path, file_name, file_type, encoding):
    file_path = os.path.join(base_path, file_name)
    if file_type == 'csv':
        return pd.read_csv(file_path, dtype=str, encoding=encoding)
    elif file_type == 'xlsx':
        return pd.read_excel(file_path, dtype=str)
    else:
        raise ValueError("Tipo de arquivo não suportado")

def map_columns(df, db_columns, column_mapping):
    df = df.rename(columns=column_mapping)
    # Garantir que todas as colunas do DataFrame estejam presentes nas colunas da tabela do banco
    return df[[col for col in db_columns if col in df.columns]]

def convert_column_types(df, column_types):
    # Converte as colunas do DataFrame para os tipos correspondentes no banco
    for col, dtype in column_types.items():
        if col in df.columns:
            if dtype == 'int' or dtype == 'smallint' or dtype == 'tinyint':
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')  # Converte para tipo inteiro
            elif dtype == 'bigint':
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')  # Converte para tipo bigint
            elif dtype == 'float' or dtype == 'real':
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')  # Converte para tipo float
            elif dtype == 'decimal' or dtype == 'numeric':
                df[col] = pd.to_numeric(df[col], errors='coerce')  # Converte para decimal ou numeric
            elif dtype == 'varchar' or dtype == 'char' or dtype == 'text':
                df[col] = df[col].astype(str)  # Converte para string (varchar, text)
            elif dtype == 'date' or dtype == 'datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')  # Converte para tipo de data
            elif dtype == 'bit':
                df[col] = df[col].apply(lambda x: bool(x) if x in [0, 1] else None)  # Converte para booleano
    return df

def insert_data(cursor, conn, df, table_name):
    # Obter os tipos de dados das colunas do banco de dados
    db_column_types = get_db_column_types(cursor, table_name)
    
    # Converter os tipos de dados do DataFrame
    df = convert_column_types(df, db_column_types)
    
    placeholders = ', '.join(['?'] * len(df.columns))
    columns = ', '.join(df.columns)
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    try:
        for row in df.itertuples(index=False, name=None):
            cursor.execute(query, row)
        conn.commit()  # Commit explícito após a inserção
        print(f"{len(df)} registros inseridos com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
        conn.rollback()  # Caso ocorra erro, faz rollback

def main():
    config = load_config('config/config.json')
    
    conn = pyodbc.connect(
        f"DRIVER={{SQL Server}};"
        f"SERVER={config['db']['server']};"
        f"DATABASE={config['db']['database']};"
        f"UID={config['db']['user']};"
        f"PWD={config['db']['password']}"
    )
    cursor = conn.cursor()
    
    # Verifique se a conexão foi feita no banco correto
    cursor.execute("SELECT DB_NAME()")
    print(f"Conectado ao banco de dados: {cursor.fetchone()[0]}")
    
    db_columns = get_db_columns(cursor, config['db']['table'])
    df = load_data(config['file']['base_path'], config['file']['name'], config['file']['type'], config['file'].get('encoding', 'utf-8'))
    df = map_columns(df, db_columns, config.get('column_mapping', {}))
    
    insert_data(cursor, conn, df, config['db']['table'])
    
    # Verificar se os dados foram inseridos corretamente
    cursor.execute(f"SELECT COUNT(*) FROM {config['db']['table']}")
    print(f"Total de registros na tabela após inserção: {cursor.fetchone()[0]}")
    
    cursor.close()
    conn.close()
    print("Dados inseridos com sucesso!")

if __name__ == "__main__":
    main()
