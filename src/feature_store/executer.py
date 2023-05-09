import mysql.connector
import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from model import Analytics_table, create_tb_medalha, create_tb_gameplay

load_dotenv()
dt_start = '2021-11-01'
dt_stop = '2022-12-31'

def date_range(start,stop):
    dates = []
    dt_start = datetime.datetime.strptime(start, '%Y-%m-%d')
    dt_stop = datetime.datetime.strptime(stop, '%Y-%m-%d')    
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime('%Y-%m-%d'))
        dt_start += datetime.timedelta(days=1)
    return dates

def import_query(path):
    with open(path, 'r') as open_file:
        query = open_file.read()
    return query

def conectar_banco():
    # Conexão com o banco de dados
    conn = mysql.connector.connect(
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_DATABASE_ANALYTICS']
    )

    # Retorna a conexão para ser usada posteriormente
    return conn

try:
    # Tenta conectar ao banco de dados
    conexao = conectar_banco()
    print("Conexão bem-sucedida!")
except Exception as e:
    # Se ocorrer algum erro, exibe uma mensagem de erro
    print("Erro ao conectar ao banco de dados:", e)

#Criando função que substitue a variável no sql e chama a query:
def exec_fs_assinatura(query, date):
    query_imp = import_query(query) 
    query_exec = query_imp.format(date=date)
    cursor = conexao.cursor()
    cursor.execute(query_exec)
    resultados = cursor.fetchall()
    df = pd.DataFrame(resultados, columns=[i[0] for i in cursor.description])
    return df

# Criando a função que chama exec_fs_assinatura para cada data na lista de data e cria uma tabela
# Por fim faz a função de cada "foto"do dia da query consultada.
def exec_assinaturas(query, dates):
    result = []
    for date in dates:
        df = exec_fs_assinatura(query, date)
        result.append(df)
    return pd.concat(result)

def exec_fs_medalha(query, date):
    query_imp = import_query(query)
    query_exec = query_imp.format(date=date)
    cursor = conexao.cursor()
    cursor.execute(query_exec)
    resultados = cursor.fetchall()
    df = pd.DataFrame(resultados, columns=[i[0] for i in cursor.description])
    return df

def exec_medalha(query, dates):
    result = []
    for date in dates:
        df = exec_fs_medalha(query, date)
        result.append(df)
    return pd.concat(result)

def exec_fs_gameplay(query, date):
    query_imp = import_query(query)
    query_exec = query_imp.format(date=date)
    cursor = conexao.cursor()
    cursor.execute(query_exec)
    resultados = cursor.fetchall()
    df = pd.DataFrame(resultados, columns=[i[0] for i in cursor.description])
    return df

def exec_gameplay(query, dates):
    result = []
    for date in dates:
        df = exec_fs_gameplay(query, date)
        result.append(df)
    return pd.concat(result)


if __name__ == "__main__":    

    # Variavel de data range
    dates = date_range(dt_start,dt_stop)
    #Variáveis de conexão
    conn = Analytics_table()
    get_session, get_engine = conn.start()


    # # Executa tabela assinatura
    df = exec_assinaturas('fs.assinatura.sql',dates)
    Analytics_table.get_assinaturas(df,get_engine)

    # # Executa tabela medalha
    df_medal = exec_medalha('fs.medalha.sql',dates)
    create_tb_medalha.get_medalhas(df_medal,get_engine)

    # Executa tabela gameplay
    # df_gameplay = exec_gameplay('fs_gameplay.sql', dates)
    # create_tb_gameplay.get_gameplay(df_gameplay, get_engine)

