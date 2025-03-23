import os
import pyodbc 
import pandas as pd
import time

#cnxn = pyodbc.connect (
#    "Driver = {SQL Server};"
#    "Server = SRVXIAWEBSQL;"
#    "Database = DBSIPOL;"
#    "Trusted_Connection=yes;"
#)


# Configuração da conexão com o banco SERVIDORX
SERVER =  "SERVIDORX"
DATABASE = "DATABASEC"
DRIVER = "SQL Server"
conn = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};uid=ususql;pwd=ususql;'

# Nome da tabela e colunas
# Nome da tabela
tabela_name = "TABELA_1"
colums = [
    "COLUNA1", 
    "COLUNA2", 
    "COLUNA3", 
    "COLUNA4", 
    "COLUNA5", 
    "COLUNA6", 
    "COLUNA7"
]

#Conexão com o banco e leitura de dados
def read_table_to_csv(connection_string, table_name, columns, output_path):
    print("Conectando ao banco de dados e iniciando a leitura...")

   

    with pyodbc.connect(conn) as con:
        cursor = con.cursor()

        #Criar consulta SQL 
        query = f"SELECT {','.join(columns)} FROM dbo.{tabela_name};"
        start_query_execution = time.perf_counter()
        cursor.execute(query)
        end_query_execution =  time.perf_counter()
        print(f"Tempo para executar a query: {end_query_execution - start_query_execution:.6f} segundos")

        #carregar todos os registros
        start_fetching = time.perf_counter()
        rows = cursor.fetchall()
        end_fetching = time.perf_counter() 

        #Converter para DataFrame 
        df = pd.DataFrame.from_records(rows, columns=columns)

        #Garantir que a subpasta 'tabelas' exista
        os.makedirs(output_path, exist_ok=True)

        #Salvar os dados em CSV
        output_file = os.path.join(output_path, f"{table_name}.csv")
        df.to_csv(output_file, index=False, sep=";")
        print(f"Dados salvos em: {output_file}")

#Caminho de saída para salvar o CSV
output_path = os.path.join(os.getcwd(), "tabelas")

#Executar o script
read_table_to_csv(conn, tabela_name, colums, output_path)