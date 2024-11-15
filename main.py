import psycopg2
import time
import sys

# Configurações da conexão
host = "192.168.1.8"
port = 5432
user = "postgres"
password = "1234"
database = "aula"

def exibir_valores_cliente(cursor, cliente_id):
    cursor.execute(f"SELECT IDCliente, Nome, Limite FROM Cliente WHERE IDCliente = {cliente_id}")
    cliente = cursor.fetchone()
    if cliente:
        print(f"Cliente selecionado - ID: {cliente[0]}, Nome: {cliente[1]}, Limite: {cliente[2]}")
    return cliente

def buscar_clientes(cursor):
    cursor.execute("SELECT IDCliente, Nome, Limite FROM Cliente")
    clientes = cursor.fetchall()
    print("Lista de Clientes:")
    for cliente in clientes:
        print(f"ID: {cliente[0]}, Nome: {cliente[1]}, Limite: {cliente[2]}")
    return clientes

# Solicita o cliente e o valor de acréscimo
try:
    # Conectando ao banco de dados PostgreSQL
    connection = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    connection.autocommit = False
    cursor = connection.cursor()

    # Lista e permite o usuário escolher o cliente
    clientes = buscar_clientes(cursor)
    print("\n")
    cliente_escolhido = int(input("Digite o ID do cliente que deseja aumentar o limite: "))
    acrescimo_limite = float(input("Digite o valor a ser acrescentado ao limite: "))
    print("\n")

    # Exibe os dados do cliente escolhido e armazena os dados atuais
    dados_iniciais = exibir_valores_cliente(cursor, cliente_escolhido)
    if not dados_iniciais:
        print("Cliente não encontrado.")
        sys.exit(1)

    while True:
        try:
            # Tenta bloquear apenas a linha do cliente selecionado
            cursor.execute(f"SELECT IDCliente, Nome, Limite FROM Cliente WHERE IDCliente = {cliente_escolhido} FOR UPDATE NOWAIT")

            # Verifica se os dados ainda são os mesmos
            dados_atuais = exibir_valores_cliente(cursor, cliente_escolhido)
            if dados_atuais != dados_iniciais:
                print("O registro do cliente já foi alterado por outra transação.")
                connection.rollback()  # Cancela a transação se os dados forem diferentes
                break  # Sai do loop e termina a execução

            # Realiza a atualização do limite
            query = f"UPDATE Cliente SET Limite = Limite + {acrescimo_limite} WHERE IDCliente = {cliente_escolhido}"
            cursor.execute(query)
            print("Query executada com sucesso. Aguardando confirmação para commit.")

            # Confirmação do usuário para commit
            resposta = input("Deseja confirmar a transação? (S/N): ").strip().upper()
            if resposta == 'S':
                connection.commit()
                print("Transação confirmada com sucesso.")
                exibir_valores_cliente(cursor, cliente_escolhido)  # Exibe os valores após o commit
            else:
                connection.rollback()
                print("Transação cancelada.")

            break  # Sai do loop após o primeiro cliente completar a transação

        except psycopg2.OperationalError:
            # Caso o bloqueio falhe, aguarda e tenta novamente
            print("Outra transação está em andamento para esse cliente. Aguardando liberação do bloqueio...")
            connection.rollback()  # Garante o rollback da transação anterior
            time.sleep(1)  # Aguarda 1 segundo antes de tentar novamente

except Exception as pg_error:
    print(f"Erro ao conectar no PostgreSQL: {pg_error}")
    if connection:
        connection.rollback()  # Garante o rollback em caso de erro
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()