# Wikis / Docs usados para base
# https://www.w3schools.com/python/default.asp
# https://medium.com/analytics-vidhya/crud-operations-in-mongodb-using-python-49b7850d627e
# https://www.mongodb.com/docs/manual/reference/operator/update/addToSet/
import pymongo
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId

client = pymongo.MongoClient(
    "mongodb+srv://zeropirata:123@atlascluster.djqn7mq.mongodb.net/?retryWrites=true&w=majority",
    server_api=ServerApi('1'))

global mydb
mydb = client.MercadoLivre
"""Insert """


def insertVendedor(nome, email):
    global mydb
    mycol = mydb.vendedor
    mydict = {"nome": nome, "email": email}
    x = mycol.insert_one(mydict)
    print(x.inserted_id)


def insertProduto(nome, preco, descricao, vendedor):
    global mydb
    columnVendedores = mydb.vendedor
    columnProdutos = mydb.produto
    findVendedor = columnVendedores.find({"nome": {
        "$eq": vendedor
    }}, {
        "_id": 1,
        "nome": 1
    })
    createDict = {}
    for fornecedor in findVendedor:
        createDict.update(fornecedor)
    if not createDict:
        print("Vendedor não existe, por favor coloque um existente")
    else:
        novoProduto = {
            "nome": nome,
            "preco": preco,
            "descricao": descricao,
            "vendedor": createDict
        }
        insert = columnProdutos.insert_one(novoProduto)
        print("Produto Inserido com ID: ", insert.inserted_id)


def insertUsuario(nome, email):
    global mydb
    mycol = mydb.usuario
    mydict = {"nome": nome, "email": email}
    x = mycol.insert_one(mydict)
    print(x.inserted_id)


def insertCompra(usuario, pagamento):
    global mydb
    columnProduto = mydb.produto
    columnUsuario = mydb.usuario
    columnCompra = mydb.compra
    objInstance = ObjectId(usuario)
    findUser = columnUsuario.find({"_id": objInstance}, {
        "_id": 1,
        "nome": 1,
        "email": 1
    })
    ok = True
    produtosAlvo = []
    compraValor = 0
    while ok:
        produtos = input("Adicionar Produto? Y/N: ")
        if produtos == "Y" or produtos == "y":
            findSortProduto()
            idProduto = input("ID do produto: ")
            print(idProduto)
            objInstance = ObjectId(idProduto)
            findProdutos = columnProduto.find({"_id": objInstance}, {
                "_id": 1,
                "nome": 1,
                "preco": 1
            })
            for x in findProdutos:
                produtosAlvo.append(x)
        elif produtos == 'N' or produtos == "n":
            iniciador = 0
            while iniciador < len(produtosAlvo):
                compraValor += int(produtosAlvo[iniciador]['preco'])
                iniciador += 1
            for user in findUser:
                newCompra = [{
                    "usuario": user,
                    "produto": produtosAlvo,
                    "total": compraValor,
                    "pagamento": pagamento
                }]
                columnCompra.insert_many(newCompra)
            ok = False


"""Update"""


def updateClienteDados(idAlvo, nome, email):
    global mydb
    mycol = mydb.usuario
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {"$set": {"nome": nome, "email": email}}
    mycol.update_one(myquery, newValues)


def updateClienteFavoritos(idAlvo, idProduto):
    global mydb
    columnProduto = mydb.produto
    columnUser = mydb.usuario
    objInstance = ObjectId(idProduto)
    myquery = {"_id": objInstance}
    findProdutos = columnProduto.find({"_id": objInstance}, {
        "_id": 1,
        "nome": 1,
        "preco": 1
    })
    objInstance = ObjectId(idAlvo)
    findUser = {"_id": objInstance}
    for produto in findProdutos:
        insertFavorito = {"$addToSet": {"favorito": produto}}
        columnUser.update_one(findUser, insertFavorito)


def updateVendedor(idAlvo, nome, email):
    global mydb
    mycol = mydb.vendedor
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {"$set": {"nome": nome, "email": email}}
    mycol.update_one(myquery, newValues)


def updateProduto(idAlvo, nome, preco, descricao):
    global mydb
    mycol = mydb.produto
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {
        "$set": {
            "nome": nome,
            "preco": preco,
            "descricao": descricao
        }
    }
    mycol.update_one(myquery, newValues)


"""Delete"""


def deleteUser(alvo):
    global mydb
    userColumn = mydb.usuario
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    userColumn.delete_one(myquery)


def deleteProduto(alvo):
    global mydb
    produtoColumn = mydb.produto
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    produtoColumn.delete_one(myquery)


def deleteVendedor(alvo):
    global mydb
    vendedorColumn = mydb.vendedor
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    vendedorColumn.delete_one(myquery)


def deleteCompra(alvo):
    global mydb
    compraColumn = mydb.compra
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    compraColumn.delete_one(myquery)


"""Querys"""


def findSortUser():
    global mydb
    userColumn = mydb.usuario
    mydoc = userColumn.find({}, {
        "nome": 1,
        "_id": 1,
    }).sort("nome")
    for result in mydoc:
        print(result)


def findSortCompras():
    global mydb
    comprasColumn = mydb.compra
    mydoc = comprasColumn.find({}, {
        "usuario": {
            "nome": 1
        },
        "produto": {
            "nome": 1,
            "preco": 1,
        }
    }).sort("nome")
    for result in mydoc:
        print(result)


def findSortProduto():
    global mydb
    columnProduto = mydb.produto
    mydoc = columnProduto.find({}, {
        "nome": 1,
        "_id": 1,
    }).sort("nome")
    for result in mydoc:
        print(result)


def findSortVendedores():
    global mydb
    vendedorColumn = mydb.vendedor
    mydoc = vendedorColumn.find({}, {
        "nome": 1,
        "_id": 1,
    }).sort("nome")
    for result in mydoc:
        print(result)


def findQueryUser(alvo):
    global mydb
    mycol = mydb.usuario
    myquery = {"nome": {"$eq": alvo}}
    mydoc = mycol.find(myquery)
    for result in mydoc:
        print(result)


def findQueryProduto(alvo):
    global mydb
    produtoColumn = mydb.produto
    myquery = {"nome": {"$eq": alvo}}
    mydoc = produtoColumn.find(myquery)
    for result in mydoc:
        print(result)


def findQueryVendedor(alvo):
    global mydb
    vendedorColumn = mydb.vendedor
    myquery = {"nome": {"$eq": alvo}}
    mydoc = vendedorColumn.find(myquery)
    for result in mydoc:
        print(result)


def findQueryCompras(alvo):
    global mydb
    comprasColumn = mydb.compra
    myquery = {"usuario.nome": {"$eq": alvo}}
    mydoc = comprasColumn.find(myquery)
    for result in mydoc:
        print(result)


def main():
    print("""
    1 - Insert de Produto \n 
    2 - Insert de Usuario \n
    3 - Insert de Vendedor \n
    4 - Update dos Dados do Cliente \n
    5 - Colocar Itens nos Favoritos \n
    6 - Listagem dos Usuarios \n
    7 - Listagem dos Produtos \n
    8 - Listagem dos Vendedores \n
    9 - Listagem das Compras \n
    10 - Achar Usuario \n
    11 - Achar Produto \n
    12 - Achar Vendedor \n
    13 - Achar Compra pelo usuario \n
    14 - Fazer uma compra \n
    15 - Deletar Usuario \n
    16 - Deletar Produto \n
    17 - Deletar Vendedor \n
    18 - Deletar Compra \n
    19 - Update Vendedor \n
    20 - Update Produto \n
    X - Fechar \n
    """)
    on = True
    while on:
        select = input("Qual opção deseja?: ")
        if select == "1":
            nomeProduto = input("Nome do produto: \n ")
            precoProduto = input("Preço do produto: \n ")
            descProduto = input("Descrição do produto: \n ")
            vendedorProduto = input("Fornecdor do produto: \n ")
            insertProduto(nomeProduto, precoProduto, descProduto,
                          vendedorProduto)
        elif select == "2":
            nomeUser = input("Nome do usuario: \n")
            emailUser = input("E-mail do usuario: \n")
            insertUsuario(nomeUser, emailUser)
        elif select == "3":
            nomeVendedor = input("Nome do vendedor: \n")
            emailVendedor = input("E-mail do vendedor: \n")
            insertVendedor(nomeVendedor, emailVendedor)
        elif select == "4":
            findSortUser()
            selectUser = input("Escolha o usuario pelo ID: \n")
            updateNomeUser = input("Digite o novo nome: \n")
            updateEmailUser = input("Digite o novo e-mail: \n")
            updateClienteDados(selectUser, updateNomeUser, updateEmailUser)
        elif select == "5":
            findSortUser()
            userAlvo = input("Escolha o usuario pelo ID: \n")
            findSortProduto()
            produtoAlvo = input("Escolha o produto pelo ID: \n")
            updateClienteFavoritos(userAlvo, produtoAlvo)
        elif select == "6":
            findSortUser()
        elif select == "7":
            findSortProduto()
        elif select == "8":
            findSortVendedores()
        elif select == "9":
            findSortCompras()
        elif select == "10":
            alvo = input("Nome do usuario: \n")
            findQueryUser(alvo)
        elif select == "11":
            alvo = input("Nome do produto: \n")
            findQueryProduto(alvo)
        elif select == "12":
            alvo = input("Nome do vendedor: \n")
            findQueryVendedor(alvo)
        elif select == "13":
            alvo = input("Nome do usuario: \n")
            findQueryCompras(alvo)
        elif select == "14":
            findSortUser()
            alvo = input("ID do usuario: \n")
            pagamento = input("Metodo de pagamento: \n")
            insertCompra(alvo, pagamento)
        elif select == "15":
            findSortUser()
            alvo = input("Id do Usuario: \n")
            deleteUser(alvo)
        elif select == "16":
            findSortUser()
            alvo = input("ID do Produto: \n")
            deleteProduto(alvo)
        elif select == "17":
            findSortUser()
            alvo = input("ID do Vendedor: \n")
            deleteVendedor(alvo)
        elif select == "18":
            alvo = input("ID da compra: \n")
            deleteCompra(alvo)
        elif select == "19":
            findSortVendedores()
            alvo = input("Selecione o vendedor pelo ID: \n ")
            novoNome = input("Digite o novo nome: \n")
            novoEmail = input("Digite o novo e-mail: \n")
            updateVendedor(alvo, novoNome, novoEmail)
        elif select == "20":
            alvo = input("Selecione o produto pelo ID: \n")
            novoNome = input("Digite o novo nome: \n")
            novoPreco = input("Digite o novo preço: R$")
            novaDesc = input("Nova descrição: \n")
            updateProduto(alvo, novoNome, novoPreco, novaDesc)
        elif select == "X":
            on = False


main()