# Wikis / Docs usados para base
# https://www.w3schools.com/python/default.asp
# https://medium.com/analytics-vidhya/crud-operations-in-mongodb-using-python-49b7850d627e
# https://www.mongodb.com/docs/manual/reference/operator/update/addToSet/
# Redis
# https://redis-py.readthedocs.io/en/stable/examples/search_json_examples.html
# https://stackoverflow.com/questions/64067197/python3-redis-redis-exceptions-responseerror-unknown-command-json-set
# https://koalatea.io/python-redis-hash/
# https://redis.io/commands/set/
from base64 import decode
from genericpath import exists
import os
import pymongo
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import redis
import json

clearConsole = lambda: os.system('cls'
                                 if os.name in ('nt', 'dos') else 'clear')
conR = redis.Redis(host='redis-13542.c275.us-east-1-4.ec2.cloud.redislabs.com',
                   port=13542,
                   password='qR58kVZOGUkWSQ5M5WTs3bCZ3mwMAujk')
client = pymongo.MongoClient(
    "mongodb+srv://zeropirata:123@atlascluster.djqn7mq.mongodb.net/?retryWrites=true&w=majority",
    server_api=ServerApi('1'))

global mydb
mydb = client.MercadoLivre
"""Insert """


def insertVendedor(nome, email):
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
    mydict = {"nome": nome, "email": email, "favorito": []}
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
    objInstance = ObjectId(alvo)
    myquery = {"_id": {"$eq": alvo}}
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
    objInstance = ObjectId(alvo)
    myquery = {"_id": {"$eq": alvo}}
    mydoc = vendedorColumn.find(myquery)
    for result in mydoc:
        print(result)


def findSortUserRedis():
    global mydb
    userColumn = mydb.usuario
    mydoc = userColumn.find({}, {
        "nome": 1,
        "_id": 0,
    }).sort("nome")
    for result in mydoc:
        print(result)


def findQueryCompras(alvo):
    global mydb
    comprasColumn = mydb.compra
    myquery = {"usuario.nome": {"$eq": alvo}}
    mydoc = comprasColumn.find(myquery)
    for result in mydoc:
        print(result)


# Redis


def getAlvoRedis(alvo):
    global mydb
    mycol = mydb.usuario
    myquery = {"nome": {"$eq": alvo}}
    mydoc = mycol.find(myquery)
    createDic = {}
    usuarios = []
    dados = conR.keys()
    for result in mydoc:
        createDic.update(result)
        createDic.pop("nome")
        createDic.pop("_id")
    if len(createDic["favorito"]) == 0:
        print()
    else:
        for produtoId in createDic["favorito"]:
                produtoId.pop("_id")
    for usuariosRedis in dados:
        usuarios.append(usuariosRedis.decode())
    user = 'user:' + createDic['email']
    if len(dados) == 0:
        conR.hset('fav:' + createDic["email"], str('favoritos'),
                  json.dumps(createDic["favorito"]))
        conR.hset('user:' + createDic["email"], str('loggin'), 'off')
        print('Usuario Criado com sucesso!')
    else:
        if user in usuarios:
            print(f"O usuario: {alvo}. Já está inserido dentro do redis...")
        else:
            conR.hset('fav:' + createDic["email"], str('favoritos'),
                      json.dumps(createDic["favorito"]))
            conR.hset('user:' + createDic["email"], str('loggin'), 'off')
            print("Inserido com sucesso")


def adicionarFavoritoRedis(alvo, idProduto):
    columnProduto = mydb.produto
    objInstance = ObjectId(idProduto)
    findProdutos = columnProduto.find({"_id": objInstance}, {
        "_id": 0,
        "nome": 1,
        "preco": 1
    })
    userFavoritos = json.loads(conR.hget(f'fav:' + alvo, 'favoritos'))
    arcane = []
    for redisFavoritos in userFavoritos:
        arcane.append(redisFavoritos)
    for produto in findProdutos:
        arcane.append(produto)
        conR.hset(f'fav:{alvo}', 'favoritos', json.dumps(arcane))


def sincronizarRedisMongo(alvo):
    global mydb
    columnUser = mydb.usuario
    myquery = {"email": {"$eq": alvo}}
    userFavoritos = json.loads(conR.hget(f'fav:' + alvo, 'favoritos'))
    arcane = []
    for redisFavoritos in userFavoritos:
        arcane.append(redisFavoritos)
    for misftToys in arcane:
        insertFavorito = {"$addToSet": {"favorito": misftToys}}
        columnUser.update_one(myquery, insertFavorito)
    print("Sincronizado com sucesso")


def listKeys():
    dados = conR.keys()
    for usuariosRedis in dados:
        decodedObj = usuariosRedis.decode()
        if decodedObj.__contains__('user:'):
            print(decodedObj.replace('user:', 'Email: '))


def listFavoritosUser(alvo):
    dados = conR.hget(f'fav:bicho@nunes', 'favoritos')
    print(dados)


def removerUser(alvo):
    conR.delete('fav:' + alvo)
    conR.delete('user:' + alvo)
    print('Removido com sucesso')


def loginRedis(alvo):
    login = f'logged:{alvo}'
    timeToLeft = conR.ttl(login)
    if timeToLeft <= -1:
        print("Deslogdado...")
        conR.hset(f'logged:{alvo}', 'timeExit', 0)
        conR.hset(f'user:{alvo}', 'loggin', 'off')
        efetuarLogin = input("Deseja logar? S/N? ")
        if efetuarLogin == 'S':
            conR.expire('logged:bicho@nunes', 15)
        else:
            print("Saindo...")
    else:
        print(
            f"Você já está logado...\nVocê tem {conR.ttl(login)}s segundos para ser deslogado..."
        )
        conR.hset(f'logged:{alvo}', 'loggin', 'on')


def main():
    clearConsole()
    print(""" 
        1 - Mongo
        2 - Redis 
     """)
    select = input("Qual opção deseja?: ")
    if select == '1':
        mongo()
    elif select == '2':
        redisAtv()
    else:
        print("Opção Não entendida")


def mongo():
    clearConsole()
    on = True
    while on:
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
        cls - Clear Console
        """)
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
        elif select == "cls":
            clearConsole()
            return mongo()
        elif select == "X":
            on = False
            main()


def redisAtv():
    clearConsole()
    on = True
    while on:
        print("""
            1 - Inserir Usuario no Redis
            2 - Adicionar item no Favorito
            3 - Listagem dos Usuarios já pertencentes Redis
            4 - Remover Usuario do Redis
            5 - Listar itens no favorito
            6 - Sincronizar Usuario no Mongo
            7 - Login Redis
            X - Fechar
            cls - Clear console
        """)
        select = input("Qual opção deseja?: ")
        if select == '1':
            findSortUserRedis()
            alvo = input("Insira o nome da pessoa: ")
            getAlvoRedis(alvo)
        elif select == '2':
            listKeys()
            alvo = input("Insira o Email da pessoa: ")
            findSortProduto()
            produtoAlvo = input("Escolha o produto pelo ID: \n")
            adicionarFavoritoRedis(alvo, produtoAlvo)
        elif select == '3':
            listKeys()
        elif select == '4':
            listKeys()
            alvo = input("Insira o Email da pessoa: ")
            removerUser(alvo)
        elif select == '5':
            listKeys()
            alvo = input("Insira o Email da pessoa: ")
            listFavoritosUser(alvo)
        elif select == '6':
            listKeys()
            alvo = input("Insira o Email da pessoa: ")
            sincronizarRedisMongo(alvo)
        elif select == '7':
            listKeys()
            alvo = input("Insira o Email da pessoa: ")
            loginRedis(alvo)
        elif select == "X":
            on = False
            main()
        elif select == "cls":
            clearConsole()
            return redisAtv()
        else:
            print('Opção não entendida, por favor digite novamente')


main()
