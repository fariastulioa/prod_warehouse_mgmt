import mysql.connector
from getpass import getpass
from mysql.connector import connect, Error
import pandas as pd
from flask import Flask,render_template,request,redirect,url_for,flash, make_response, Response, send_file
import os
import time

# TABELAS:

# containers = id_container, local, tipo
# lotes = id_lote, id_container, id_produto, quantidade
# transacoes = id_transacao, id_produto, id_container, quantidade, quando, tipo (0,1,2,3)
# TIPOS? : retirou, recebeu, vendeu, comprou
# produtos = id_produto, nome, custo, preco

with connect(
    host='localhost',
    user='root',
    password='17966971'
) as connie:
    c = connie.cursor()
    c.execute("""SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));""")


def execute_sql(sql_string): # funcao para executar uma transacao atomica no database
    query_results = []
    print(sql_string)
    try:
        with connect(
            host='localhost',
            user="root", # root ou input("enter username: ")
            password="17966971", # 17966971 ou getpass("enter password: ")
            database='management'
        ) as connie:

            c = connie.cursor()
            c.execute(sql_string)
            query_results.extend( c.fetchall() )
            connie.commit()
            c.close()
            connie.close()

    except Error as e:
        print(e)
    finally:
        return(query_results)



def get_nome(id):
    nome = execute_sql(f"""
        SELECT nome FROM produtos WHERE id_produto = {id};
    """)
    return(nome[0][0])

def get_quant_total(nome_prod):
    total = execute_sql(f"""
        SELECT nome, SUM(quantidade) as 'total' FROM lotes INNER JOIN produtos USING (id_produto) GROUP BY nome HAVING nome = '{nome_prod}' ORDER BY total DESC;
    """)
    return(total[0])


def get_mvp(n):
    mvps = execute_sql(f"""
        SELECT nome, preco*quantidade AS 'receita' FROM lotes INNER JOIN produtos USING (id_produto) GROUP BY nome ORDER BY receita DESC LIMIT {n};
    """)
    return(mvps)
mvp_string = f"""
        SELECT nome, preco*quantidade AS 'receita' FROM lotes INNER JOIN produtos USING (id_produto) GROUP BY nome ORDER BY receita DESC LIMIT 10;
    """


def get_mvc(n):
    mvcs = execute_sql(f"""
        SELECT id_container, local, tipo, t.valor_guardado FROM containers
        INNER JOIN (SELECT id_container, SUM(quantidade * preco) AS 'valor_guardado' FROM lotes INNER JOIN produtos USING (id_produto) GROUP BY id_container ORDER BY valor_guardado DESC LIMIT {n}) t
        USING (id_container);
    """)
    return(mvcs)
mvc_string = f"""
        SELECT id_container, local, tipo, t.valor_guardado FROM containers
        INNER JOIN (SELECT id_container, SUM(quantidade * preco) AS 'valor_guardado' FROM lotes INNER JOIN produtos USING (id_produto) GROUP BY id_container ORDER BY valor_guardado DESC LIMIT 10) t
        USING (id_container);
    """


def query_all(tabela):
    query = execute_sql(f"""
        SELECT * FROM {tabela};
    """)
    return(query)

def pd_df(query, columns):
    return(pd.DataFrame(query, columns=columns))

def df_from_sql(query):
    with connect(
    host='localhost',
    user='root',
    password='17966971',
    database='management'
    ) as connie:
        df = pd.read_sql_query(query, connie)
        print(df.head())
        print(type(df))
    return(df)


df_mvps = pd_df(get_mvp(10), ['nome', 'receita_total'])
df_mvps['receita_total'].map('{:,.2f}'.format)



df_mvcs = pd_df(get_mvc(10), ['id_container', 'local', 'tipo_container', 'valor_total_armazenado'])
df_mvcs['valor_total_armazenado'].map('{:,.2f}'.format)

print(df_mvps)
print(df_mvcs)


def ajusta_precos(fator):
    execute_sql(f"""
        UPDATE produtos SET preco = ROUND((preco * {fator}), 2);
    """)
    print("Precos ajustados com sucesso")


def muda_preco(id_prod, novo_preco):
    execute_sql(f"""
        UPDATE produtos SET preco = {novo_preco} WHERE id_produto = {id_prod};
    """)
    print("Preco ajustado com sucesso")


def get_prod_id(nome):
    prod_id = execute_sql(f"""
        SELECT id_produto FROM produtos WHERE nome = '{nome}';
    """)
    return(prod_id[0][0])


def get_cont_id(local, tipo):
    cont_id = execute_sql(f"""
        SELECT id_container FROM containers WHERE local = '{local}' AND tipo = '{tipo}';
    """)
    return(cont_id[0][0])


def movimenta(id_lote_origem, id_container_destino):
    
    id_cont_origem, id_prod, quant = execute_sql(f"""
        SELECT id_container, id_produto, quantidade FROM lotes WHERE id_lote = {id_lote_origem};
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
    time.sleep(0.05)
    execute_sql(f"""
        INSERT INTO transacoes (id_produto, id_container, quantidade, tipo) VALUES ({id_prod}, {id_cont_origem}, {quant}, '{retirou}');
    """)
    time.sleep(0.05)
    execute_sql(f"""
        INSERT INTO transacoes (id_produto, id_container, quantidade, tipo) VALUES ({id_prod}, {id_container_destino}, {quant}, '{recebeu}');
    """)
    time.sleep(0.05)
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
        time.sleep(0.05)
        execute_sql(f"""
            INSERT INTO transacoes (id_produto, id_container, quantidade, tipo) VALUES ({id_prod}, {id_cont}, {quantidade}, '{tipo}');
        """)
        time.sleep(0.05)
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
    time.sleep(0.05)
    execute_sql(f"""
        INSERT INTO transacoes (id_produto, id_container, quantidade, tipo) VALUES ({id_prod}, {id_container}, {quantidade}, '{tipo}');
    """)
    time.sleep(0.05)
    
    print("Compra concluida")


app = Flask(__name__)
app.secret_key = os.urandom(24)

@app.route("/")
@app.route("/home")
def home():

    return render_template("home.html", **request.args)

@app.route("/buying", methods=['POST', 'GET'])
def buying():
    
    if request.method=='POST':
        
        NomeProduto = request.form['NomeProduto']
        IDContDestino = int(request.form['IDContDestino'])
        Quantidade = float(request.form['Quantidade'])
        
        compra(IDContDestino, NomeProduto, Quantidade)
        
        message = "Compra registrada com sucesso!"
        
        return(redirect(url_for("home", message=message)))
    
    else:
        
        prod = df_from_sql("SELECT * FROM produtos;")
        prod['id_produto'] = prod['id_produto'].astype('int')
        lote = df_from_sql("SELECT * FROM lotes;")
        lote['id_lote'] = lote['id_lote'].astype('int')
        cont = df_from_sql("SELECT * FROM containers;")
        cont['id_container'] = cont['id_container'].astype('int')
        tran = df_from_sql("SELECT * FROM transacoes;")
        tran['id_transacao'] = tran['id_transacao'].astype('int')
        return render_template("buying.html", col_tran=tran.columns.values, row_tran=list(tran.values.tolist()),
                            col_cont=cont.columns.values, row_cont=list(cont.values.tolist()),
                            col_lote=lote.columns.values, row_lote=list(lote.values.tolist()),
                            col_prod=prod.columns.values, row_prod=list(prod.values.tolist()),
                            zip=zip)

@app.route("/moving", methods=['POST', 'GET'])
def moving():
    
    if request.method=='POST':
        IDLoteOrigem = int(request.form['IDLoteOrigem'])
        IDContDestino = int(request.form['IDContDestino'])
        
        movimenta(IDLoteOrigem, IDContDestino)
        message = "Movimentação registrada com sucesso!"
        
        return(redirect(url_for("home", message=message)))
    else:
        prod = df_from_sql("SELECT * FROM produtos;")
        prod['id_produto'] = prod['id_produto'].astype('int')
        lote = df_from_sql("SELECT * FROM lotes;")
        lote['id_lote'] = lote['id_lote'].astype('int')
        cont = df_from_sql("SELECT * FROM containers;")
        cont['id_container'] = cont['id_container'].astype('int')
        tran = df_from_sql("SELECT * FROM transacoes;")
        tran['id_transacao'] = tran['id_transacao'].astype('int')
        return render_template("moving.html", col_tran=tran.columns.values, row_tran=list(tran.values.tolist()),
                            col_cont=cont.columns.values, row_cont=list(cont.values.tolist()),
                            col_lote=lote.columns.values, row_lote=list(lote.values.tolist()),
                            col_prod=prod.columns.values, row_prod=list(prod.values.tolist()),
                            zip=zip)

@app.route("/selling", methods=['POST', 'GET'])
def selling():
    
    if request.method=='POST':
        
        IDLoteOrigem = int(request.form['IDLoteOrigem'])
        Quantidade = float(request.form['Quantidade'])
        
        vende(IDLoteOrigem, Quantidade)
        message = "Venda registrada com sucesso!"
        
        return(redirect(url_for("home", message=message)))
    
    else:
    
        prod = df_from_sql("SELECT * FROM produtos;")
        prod['id_produto'] = prod['id_produto'].astype('int')
        lote = df_from_sql("SELECT * FROM lotes;")
        lote['id_lote'] = lote['id_lote'].astype('int')
        cont = df_from_sql("SELECT * FROM containers;")
        cont['id_container'] = cont['id_container'].astype('int')
        tran = df_from_sql("SELECT * FROM transacoes;")
        tran['id_transacao'] = tran['id_transacao'].astype('int')
        return render_template("selling.html", col_tran=tran.columns.values, row_tran=list(tran.values.tolist()),
                            col_cont=cont.columns.values, row_cont=list(cont.values.tolist()),
                            col_lote=lote.columns.values, row_lote=list(lote.values.tolist()),
                            col_prod=prod.columns.values, row_prod=list(prod.values.tolist()),
                            zip=zip)


@app.route("/table_containers")
def table_containers():
    df = df_from_sql("SELECT * FROM containers;")
    df['id_container'] = df['id_container'].astype('int')
    return render_template("table_containers.html", column_names=df.columns.values, row_data=list(df.values.tolist()),link_column='id_container', zip=zip)

@app.route("/table_lotes")
def table_lotes():
    df = df_from_sql("SELECT * FROM lotes;")
    df['id_lote'] = df['id_lote'].astype('int')
    return render_template("table_lotes.html", column_names=df.columns.values, row_data=list(df.values.tolist()),link_column='id_lote', zip=zip)

@app.route("/table_produtos")
def table_produtos():
    df = df_from_sql("SELECT * FROM produtos;")
    df['id_produto'] = df['id_produto'].astype('int')
    return render_template("table_produtos.html", column_names=df.columns.values, row_data=list(df.values.tolist()),link_column='id_produto', zip=zip)

@app.route("/table_transacoes")
def table_transacoes():
    df = df_from_sql("SELECT * FROM transacoes;")
    df['id_transacao'] = df['id_transacao'].astype('int')
    return render_template("table_transacoes.html", column_names=df.columns.values, row_data=list(df.values.tolist()),link_column='id_transacao', zip=zip)

@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/stats")
def stats():
    pd.options.display.float_format = '$ {:,.2f}'.format
    mvps = df_from_sql(mvp_string)
    mvps['receita'] = mvps['receita'].map('$ {:,.2f}'.format)
    mvcs = df_from_sql(mvc_string)
    mvcs['valor_guardado'] = mvcs['valor_guardado'].map('${:,.2f}'.format)
    print(mvps.head())
    print('butico')
    print(mvcs.head())
    
    return render_template("stats.html", col_mvp=mvps.columns.values, row_mvp=list(mvps.values.tolist()),
                            col_mvc=mvcs.columns.values, row_mvc=list(mvcs.values.tolist()),
                            zip=zip, **request.args)

app.run(debug=True, port=1648)


