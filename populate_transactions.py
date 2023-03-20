import random
import numpy as np
import mysql.connector
from getpass import getpass
from mysql.connector import connect, Error
import time

user = input("enter username: ")
password = getpass("enter password: ")

disable_constraint_fk = """SET FOREIGN_KEY_CHECKS = 0;"""

try:
    with connect(
        host='localhost',
        user = user,
        password = password,
    ) as connie:
        with connie.cursor() as c:
            c.execute(disable_constraint_fk)
        connie.commit()
except Error as e:
    print(e)


def execute_sql(sql_string): # funcao para executar uma transacao atomica no database
    query_results = []
    try:
        connie =  connect(
            host='localhost',
            user = user,
            password = password,
            database="management"
        )
        c = connie.cursor()
        c.execute(sql_string)
        query_results.extend( c.fetchall() )
        c.clear_attributes()
        connie.commit()
        connie.close()
        c.close()
    except Error as e:
        print(e)
    finally:
        return(query_results)

def execute_sqls(sql_string): # funcao para executar multiplas transacoes atomicas no database
    query_results = []

    try:
        connie =  connect(
            host='localhost',
            user = user,
            password = password,
            database="management"
        )
        c = connie.cursor()
        
        c.executemany(sql_string)
        query_results.extend( c.fetchall() )
        c.clear_attributes()
        connie.commit()
        connie.close()
        c.close()


    except Error as e:
        print(e)
    finally:
        return(query_results)



def get_prod_id(nome):
    prod_id = execute_sql(f"""
        SELECT id_produto FROM produtos WHERE nome = '{nome}'
    """)
    return(prod_id[0][0])


def movimenta(id_lote_origem, id_container_destino):
    
    id_cont_origem, id_prod, quant = execute_sql(f"""
        SELECT id_container, id_produto, quantidade FROM lotes WHERE id_lote = {id_lote_origem}
    """)[0]
    id_cont_origem = int(id_cont_origem)
    id_container_destino = int(id_container_destino)
    id_lote_origem = int(id_lote_origem)
    id_prod = int(id_prod)
    if quant < 1:
        return()
    retirou = 'retirou'
    recebeu = 'recebeu'
    execute_sql(f"""
        UPDATE lotes SET id_container = {id_container_destino} WHERE id_lote = {id_lote_origem};
    """)
    time.sleep(0.1)
    execute_sql(f"""
        INSERT INTO transacoes (id_produto, id_container, quantidade, tipo) VALUES ({id_prod}, {id_cont_origem}, {quant}, '{retirou}');
    """)
    time.sleep(0.2)
    execute_sql(f"""
        INSERT INTO transacoes (id_produto, id_container, quantidade, tipo) VALUES ({id_prod}, {id_container_destino}, {quant}, '{recebeu}');
    """)
    time.sleep(0.1)
    print("Movimentacao concluida")


def vende(id_lote, quantidade):
    if quantidade < 1:
        return()
    tipo = 'vendeu'
    id_prod, id_cont, quant_antes = execute_sql(f"""
        SELECT id_produto, id_container, quantidade FROM lotes WHERE id_lote = {id_lote};
    """)[0]
    if quant_antes < 1:
        return()
    if quant_antes < quantidade:
        print("Erro! Quantidade solicitada indisponível")
    else:
        execute_sql(f"""
            UPDATE lotes SET quantidade = quantidade - {quantidade} WHERE id_lote = {id_lote};
        """)
        time.sleep(0.1)
        execute_sql(f"""
            INSERT INTO transacoes (id_produto, id_container, quantidade, tipo) VALUES ({id_prod}, {id_cont}, {quantidade}, '{tipo}');
        """)
        time.sleep(0.1)
        print("Venda concluida")
    if quant_antes == quantidade:
        execute_sql(f"""
            DELETE FROM lotes WHERE id_lote = {id_lote};
        """)
        print("Lote esgotado!")


def compra(id_container, produto, quantidade):
    if quantidade < 1:
        return()
    tipo='comprou'
    id_prod = get_prod_id(produto)
    
    execute_sql(f"""
        INSERT INTO lotes (id_container, id_produto, quantidade) VALUES ({id_container}, {id_prod}, {quantidade});
    """)
    time.sleep(0.1)
    execute_sql(f"""
        INSERT INTO transacoes (id_produto, id_container, quantidade, tipo) VALUES ({id_prod}, {id_container}, {quantidade}, '{tipo}')
    """)
    time.sleep(0.1)
    
    print("Compra concluida")


def get_container_ids():
    
    lista = execute_sql("""
                SELECT id_container FROM containers
                """)
    output= []
    for elemento in lista:
        output.append(elemento[0])
    return(output)
ids_container = get_container_ids()


def get_product_names():
    
    lista = execute_sql("""
                        SELECT nome FROM produtos
                        """)
    output= []
    for elemento in lista:
        output.append(elemento[0])

    return(output)
nomes_produto = get_product_names()


def get_lote_ids():
    
    lista = execute_sql("""
                SELECT id_lote FROM lotes
                """)
    output= []
    for elemento in lista:
        output.append(elemento[0])

    return(output)
ids_lote = get_lote_ids()


amount_range = np.arange(0.00, 1000.00, 40.25)




for i in range(0, 200, 1):
    cont_id = np.random.choice(ids_container)
    prod_nome = np.random.choice(nomes_produto)
    quantos = (np.random.choice(amount_range)/2)+10
    compra(cont_id, prod_nome, quantos)
    time.sleep(0.1)

for i in range(0,50,1):
    lote = np.random.choice(ids_lote)
    container = np.random.choice(ids_container)
    
    movimenta(lote, container)
    time.sleep(0.1)

venda_range = np.arange(0.4, 1.00, 0.10)

for i in range(0, 200, 1):
    time.sleep(0.1)
    lote = np.random.choice(ids_lote)

    try:
        quant_antes = float(execute_sql(f"""
        SELECT quantidade FROM lotes WHERE id_lote = {lote};
        """)[0][0])
        

        
        porcao_vendida = float(np.random.choice(venda_range))
        
        vende(lote, (quant_antes*porcao_vendida))
        
    except IndexError:
        print("!!! IndexError !!!")
        pass



