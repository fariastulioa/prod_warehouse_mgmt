import random
import numpy as np

# TABELAS:

# containers = id_container, local, tipo
# lotes = id_lote, id_container, id_produto, quantidade
# transacoes = id_transacao, id_produto, id_container, quantidade, quando, tipo (0 ou 1)
# produtos = id_produto, nome, custo, preco

# pega um produto, coloca custo e preco nele, deixa o mysql gerar o id
# insere todos containers possiveis
# pega quantidades aleatorias para preencher cada container gerado
# define funcao para fazer transacoes
# faz algumas transacoes aleatorias


cosmetics = ['mini', 'wide', 'slim', 'large']

prefixes = ['ab', 'desto', 'je', 'gim',
            'plamen', 'tred', 'roard']

suffixes = ['gon', 'aly', 'cras',
            'icto', 'lizy', 'nor']

editions = ['lite', 'deluxe', 'premium', 'limited edition', 'pro', 'standard']

nomes_produto = []

def get_name(co, pr, su, ed):
    return(f"{co} {pr}{su} {ed}")

for cosmetic in cosmetics:

    for prefix in prefixes:
        for suffix in suffixes:
            for edition in editions:
                nomes_produto.append(get_name(cosmetic, prefix, suffix, edition))

print(len(nomes_produto))

tipos_container = ['corredor', 'galpao', 'armazem', 'deposito']

zonas = ['norte', 'sul', 'leste', 'oeste']

andares = ['inferior', 'superior']

locais = []
for zona in zonas:
    for andar in andares:
        locais.append(f"{zona} {andar}")




fator_ganho = np.arange(1.05, 4.50, 0.05)
cost_range = np.arange(0.50, 875.50, 3.50)
amount_range = np.arange(0.00, 2000.00, 25.50)
print(type(cost_range))

import mysql.connector
from getpass import getpass
from mysql.connector import connect, Error

user = input("enter username: ")
password = getpass("enter password: ")



def execute_sql(sql_string): # funcao para executar uma transacao atomica no database
    query_results = []
    print(sql_string)
    try:
        with connect(
            host='localhost',
            user = user,
            password = password,
            database="management"
        ) as connie:
            c = connie.cursor()
            c.execute(sql_string)
            query_results.extend( c.fetchall() )
            connie.commit()

    except Error as e:
        print(e)
    finally:
        return(query_results)


# Criando o database
criar_db_string = "CREATE DATABASE management;"
deletar_db_string = "DROP DATABASE IF EXISTS management;"

try:
    with connect(
        host='localhost',
        user = user,
        password = password,
    ) as connie:
        with connie.cursor() as c:
            c.execute(deletar_db_string)
except Error as e:
    print(e)

try:
    with connect(
        host='localhost',
        user = user,
        password = password,
    ) as connie:
        with connie.cursor() as c:
            c.execute(criar_db_string)
except Error as e:
    print(e)
# Aqui, o database deve ter sido criado


criar_containers_str = """CREATE TABLE containers(
    id_container INT AUTO_INCREMENT PRIMARY KEY,
    local VARCHAR(100),
    tipo VARCHAR(100)
);"""

execute_sql(criar_containers_str)


for local in locais:
    for tipo in tipos_container:
        execute_sql(f"""
                    INSERT INTO containers (local, tipo) VALUES ('{local}', '{tipo}')
                    """)


criar_produtos_str = """CREATE TABLE produtos(
    id_produto INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100),
    preco FLOAT,
    custo FLOAT
    );"""

execute_sql(criar_produtos_str)

for nome in nomes_produto:
    custo = np.random.choice(cost_range)
    ganho = np.random.choice(fator_ganho)
    preco = custo * ganho
    
    execute_sql(f"""
                INSERT INTO produtos (nome, preco, custo) VALUES ('{nome}', {preco}, {custo})
                """)

ids_containers = execute_sql("""
                            SELECT id_container FROM containers
                            """)
cont_ids = []
for item in ids_containers:
    cont_ids.append(item[0])




ids_produtos = execute_sql("""
                        SELECT id_produto FROM produtos
                        """)
prod_ids = []
for item in ids_produtos:
    prod_ids.append(item[0])
print(prod_ids)
print(cont_ids)


create_lotes_str = """CREATE TABLE lotes(
    id_lote INT AUTO_INCREMENT PRIMARY KEY, 
    id_container INT NOT NULL,
    id_produto INT NOT NULL,
    quantidade FLOAT,
    FOREIGN KEY (id_container) REFERENCES containers(id_container),
    FOREIGN KEY (id_produto) REFERENCES produtos(id_produto)
);"""

execute_sql(create_lotes_str)
prods_per_container = np.arange(1, 25, 1)

for cont_id in cont_ids:
    n = np.random.choice(prods_per_container)
    for i in range(1, n+1, 1):
        print(i)
        amount = np.random.choice(amount_range)
        prod_id = np.random.choice(prod_ids)
        execute_sql(f"""
                    INSERT INTO lotes (id_container, id_produto, quantidade) VALUES ({cont_id}, {prod_id}, {amount})
                    """)



create_transacoes_str = """CREATE TABLE transacoes(
    id_transacao INT AUTO_INCREMENT PRIMARY KEY,
    id_produto INT NOT NULL,
    id_container INT NOT NULL,
    quantidade FLOAT,
    quando TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    tipo VARCHAR(16),
    FOREIGN KEY (id_container) REFERENCES containers(id_container),
    FOREIGN KEY (id_produto) REFERENCES produtos(id_produto)
);"""

execute_sql(create_transacoes_str)