## @package hostsSD
#  Documentation for this module.
#
#  More details.
import socket
import json
import time
import sys
import random
import string
from _thread import *

## The argc, argv
#  Verifica se os parametros de entrada de execução estão corretos
#  More details.
if len(sys.argv) != 3:
	print("Uso correto: script endereço_entrada endereço_host")
	exit()

endereco_entrada = str(sys.argv[1])
porta_entrada = 2000
endereco = str(sys.argv[2])
porta = 3000

class Host:

	## Function Iniciar
	# inicia as threads para receber novas conexões
	# conecta o host com a entrada
	# procura host para se conectar
	# verifica a possibilidade de se tornar o nó de entrada
	def __init__(self, endereco_entrada, porta_entrada, endereco, porta) -> None:
		self.endereco_entrada = endereco_entrada
		self.porta_entrada = porta_entrada
		self.endereco = endereco
		self.porta = porta

		self.conexoes = {}
		self.conexoes_entrada = {}
		self.conectado = False
		self.isEntrada = True
		self.id = None
		self.idNovo = 0
		self.idMensagem = ""
		self.vinculo = None
		self.hostSocket = None
		self.listaVotacao = []

		start_new_thread(self.__receberConexoes, ())
		start_new_thread(self.__conectarEntrada, ())
		start_new_thread(self.__manterVinculo, ())
		start_new_thread(self.__receberConexoesEntrada, ())

	## Function PING
	#  @param host que será conectado
	#  Esta função, dado uma novo host for criado, é testado com todos os hosts
	#  do Sistema (SD) e é retornado o host de menor atraso
	def __pingConexoes(self):
		menorTempo = 0
		conexao = None

		ids = list(self.conexoes_entrada.keys())
		for id in ids:
			if self.id == id:
				continue

			testeSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			time.sleep(0.5)
			print(f"Testando {self.conexoes_entrada[id]['endereco']}:{self.conexoes_entrada[id]['porta']}...")

			inicio = time.time()
			try:
				testeSocket.connect((self.conexoes_entrada[id]["endereco"], self.conexoes_entrada[id]["porta"]))
			except:
				print("Não foi possível realizar o ping")
				continue
			fim = time.time()
			body = {
				"tipo": "teste",
				"conexao": {
					"endereco": self.endereco,
					"porta": self.porta
				}
			}

			testeSocket.send(bytes(json.dumps(body, indent=4, ensure_ascii=True), "utf8"))
			testeSocket.close()

			print(f"{(fim - inicio)*1000} ms")

			if menorTempo != 0:
				if fim - inicio < menorTempo:
					menorTempo = fim - inicio
					conexao = self.conexoes_entrada[id]
					conexao['id'] = id
			else:
				menorTempo = fim - inicio
				conexao = self.conexoes_entrada[id]
				conexao['id'] = id

		return conexao

	## Function remover
	#  @param host
	#  Remove o host da lista de hosts conectados
	def __remover(self, id, conexoes):
		if conexoes.get(id) != None:
			conexoes.pop(id)

	## Function remover
	#  @param host
	#  Remove o host de entrada da lista de hosts conectados
	def __removerEntrada(self):
		ids = list(self.conexoes_entrada.keys())
		for id in ids:
			if self.conexoes_entrada[id]["entrada"]:
				self.conexoes_entrada.pop(id)
				break

	## Function repassarMensagem
	#  @param host
	# Repassa as mensagens para os destinatarios
	def __repassarMensagem(self, body, destinatarios = []):
		if len(destinatarios) == 0:
			for id in list(self.conexoes.keys()):
				try:
					self.conexoes[id].send(bytes(json.dumps(body, indent=4, ensure_ascii=True) + "\n\n", "utf8"))
				except:
					self.conexoes[id].close()

					self.__remover(id, self.conexoes)
		else:
			for id in list(destinatarios.keys()):
				if destinatarios[id]['endereco'] == self.endereco and destinatarios[id]['porta'] == self.porta:
					continue

				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

				try:
					sock.connect((destinatarios[id]["endereco"], destinatarios[id]["porta"]))
				except:
					continue

				_body = {
					"tipo": "conexao",
					"conexao": {
						"id": self.id,
						"endereco": self.endereco,
						"porta": self.porta
					}
				}

				sock.send(bytes(json.dumps(_body, indent=4, ensure_ascii=True) + "\n\n", "utf8"))
				time.sleep(0.5)
				sock.send(bytes(json.dumps(body, indent=4, ensure_ascii=True) + "\n\n", "utf8"))
				sock.close()

	## Function AdicionarVoto
	# add voto na lista de votos na hora da eleição
	def __adicionarVoto(self, voto):
		for votoHost in self.listaVotacao:
			if votoHost['quemVotou'] == voto['quemVotou']:
				return

		self.listaVotacao.append(voto)

	## Fucntion ReceberMenssagem
	#  Esta função executa a partir de uma thread e receber as mensagem dos 
	#  hosts conectados, ela verifica se a mensagem recebida ja foi repassada 
	#  por ela mesma para enviar para outro host
	def __receberMensagem(self, id):
		while True:
			try:
				_resposta = self.conexoes[id].recv(4096).decode("utf8")
				if _resposta == "":
					self.__remover(id, self.conexoes)
					if self.vinculo != None:
						print(f"{id} caiu")
					break
			except:
				self.__remover(id, self.conexoes)
				break

			_respostas = _resposta.split('\n\n')
			for _resposta in _respostas:
				if _resposta == '':
					continue
				resposta = json.loads(_resposta)

				if resposta['tipo'] == "mensagem":
					mensagem = resposta["mensagem"]

					if mensagem["endereco"] != f"{self.endereco}:{self.porta}":
						if mensagem['id'] != self.idMensagem:
							self.idMensagem = mensagem['id']
							print(f"Host {mensagem['idHost']}: {mensagem['conteudo']}")
							self.__repassarMensagem(resposta)
				elif resposta['tipo'] == "eleicao":
					self.__adicionarVoto(resposta)

	## Function procurarConexao
	#  Função que procura por uma conexão ao inicia o script de novo host
	#  @return {retorn o host que sera conectado ao novo nó}
	def __procurarConexao(self, idVinculo = -1):
		self.vinculo = None

		time.sleep(0.5)

		while self.id == None:
			continue

		if idVinculo == -1:
			while self.vinculo == None:
				self.vinculo = self.__pingConexoes()
		else:
			self.vinculo = self.conexoes_entrada[idVinculo]
			self.vinculo['id'] = idVinculo

		self.hostSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.hostSocket.connect((self.vinculo['endereco'], self.vinculo['porta']))

		body = {
			"tipo": "conexao",
			"conexao": {
				"id": self.id,
				"endereco": self.endereco,
				"porta": self.porta
			}
		}

		self.hostSocket.send(bytes(json.dumps(body, indent=4, ensure_ascii=True), "utf8"))
		print(f"Este host se conectou com o host de id {self.vinculo['id']}")
		self.conexoes[self.vinculo['id']] = self.hostSocket
		start_new_thread(self.__receberMensagem, (self.vinculo['id'],))

	## Function ManterVinculo
	# procura host para se conectar
	def __manterVinculo(self):
		vinculado = False
		procurandoConexao = False
		while True:
			if not vinculado and not procurandoConexao and self.conectado:
				start_new_thread(self.__procurarConexao, ())
				procurandoConexao = True

			time.sleep(0.5)

			vinculado = False
			if self.vinculo != None:
				if self.conexoes_entrada.get(self.vinculo['id']) != None:
					vinculado = True
					procurandoConexao = False

	## Function VotarNovaEntrada
	# Faz a eleição caso o nó de entrada caia
	# É feita uma votação por cada nó a qual eles fazem um ping entre eles, e aquele que tem o menor ping médio é eleito o nó de entrada
	# Se der empate é eleito aquele a qual tem o menor numero de porta
	# Se der empate, é eleito aquele que tem o menor id de nó
	def __votarNovaEntrada(self):
		conexao = self.__pingConexoes()
		if conexao != None:
			body = {
				"tipo": "eleicao",
				"quemVotou": self.id,
				"escolha": conexao
			}
			self.__adicionarVoto(body)
			self.__repassarMensagem(body, self.conexoes_entrada)

			while len(self.listaVotacao) < len(self.conexoes_entrada):
				time.sleep(2)
				continue

		resultado = []
		for voto in self.listaVotacao:
			if len(resultado) == 0:
				voto['escolha']['contagem'] = 1
				resultado.append(voto['escolha'])
				continue

			for res in resultado:
				if res['endereco'] == voto['escolha']['endereco'] and res['porta'] == voto['escolha']['porta']:
					res['contagem'] += 1
					break
			else:
				voto['escolha']['contagem'] = 1
				resultado.append(voto['escolha'])

		self.listaVotacao = []
		vencedor = {
			"contagem": 0,
			"id": self.id,
			"endereco": self.endereco,
			"porta": self.porta
		}

		print()
		for res in resultado:
			print(f"Host {res['id']} ({res['endereco']}:{res['porta']})")
			print(f"\tRecebeu {res['contagem']} voto(s)")
			if res['contagem'] >= vencedor['contagem']:
				if res['contagem'] == vencedor['contagem']:
					if res['porta'] <= vencedor['porta']:
						if res['porta'] == vencedor['porta']:
							if res['id'] < vencedor['id']:
								vencedor = res
						else:
							vencedor = res
				else:
					vencedor = res

		print(f"\nA nova entrada é o host de id {vencedor['id']} ({vencedor['endereco']}:{vencedor['porta']})")
		if vencedor['endereco'] == self.endereco and vencedor['porta'] == self.porta:
			self.endereco_entrada = self.endereco
			self.isEntrada = True
		else:
			self.endereco_entrada = vencedor['endereco']
			self.conexoes_entrada = {}
			time.sleep(1)
		self.conectado = True

	## Function ConectarEntrada
	# conecta o host com a entrada
	def __conectarEntrada(self):
		while True:
			while self.isEntrada or not self.conectado:
				if self.vinculo == None or self.id == None:
					continue

				self.__atualizarVinculo(self.id, self.vinculo['id'])

				time.sleep(1)
				continue

			self.entrada = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				self.entrada.connect((self.endereco_entrada, self.porta_entrada))
			except:
				time.sleep(1)
				continue

			body = {
				"tipo": "entrada",
				"conexao": {
					"endereco": self.endereco,
					"porta": self.porta
				}
			}

			self.entrada.send(bytes(json.dumps(body, indent=4, ensure_ascii=True), "utf8"))
			resposta = json.loads(self.entrada.recv(4096).decode("utf8"))
			self.id = resposta["id"]
			print(f"\nId deste host: {self.id}\n")

			while True:
				try:
					_resposta = self.entrada.recv(4096).decode("utf8")
					if _resposta == "":
						print("A entrada não está mais disponível")
						self.conectado = False
						self.vinculo = None
						self.__removerEntrada()
						self.__votarNovaEntrada()
						break
				except:
					continue

				try:
					resposta = json.loads(_resposta)
					if resposta["tipo"] == "entrada":
						self.conexoes_entrada = resposta["conexoes"]
						for vinculo in resposta["vincular"]:
							if vinculo[1] == self.id:
								self.vinculo = None
								self.__procurarConexao(vinculo[0])
								self.conexoes.pop(vinculo[0])
								break

				except:
					continue

				body = {
					"tipo": "entrada",
					"id": self.id,
					"vinculo": self.vinculo['id'] if self.vinculo != None else -1
				}

				try:
					self.entrada.send(bytes(json.dumps(body, indent=4, ensure_ascii=True), "utf8"))
				except:
					print("A entrada não está mais disponível")
					self.conectado = False
					self.vinculo = None
					self.__removerEntrada()
					self.__votarNovaEntrada()
					break
	## Function AtualizarVinculo
	# Atualiza o vinculo de um id
	def __atualizarVinculo(self, id, vinculo):
		self.conexoes_entrada[id]['vinculo'] = vinculo

	## Function ManterConexão
	#  Esta função executa a partir deu uma thread, a qual mantem os hosts 
	#  do ponto de entrada atualizados em todos os hosts do SD
	def __manterConexao(self, conexao):
		resposta = json.loads(conexao.recv(4096).decode("utf8"))
		id = str(self.idNovo)
		self.idNovo += 1

		body = {
			"id": id
		}
		conexao.send(bytes(json.dumps(body, indent=4, ensure_ascii=True), "utf8"))

		conexaoInfo = {
			"entrada": False,
			"endereco": resposta['conexao']['endereco'],
			"porta": resposta['conexao']['porta'],
			"vinculo": -1
		}
		self.conexoes_entrada[id] = conexaoInfo

		print(f"\t<Entrada> O host de id {id} ({conexaoInfo['endereco']}:{conexaoInfo['porta']}) se conectou")


		while True:
			time.sleep(0.5)
			body = {
				"tipo": "entrada",
				"conexoes": self.conexoes_entrada,
				"vincular": self.__precisamVincular()
			}
			try:
				conexao.send(bytes(json.dumps(body, indent=4, ensure_ascii=True) + "\n\n", "utf8"))
			except:
				self.__remover(id, self.conexoes_entrada)
				print(f"\t<Entrada> O host de id {id} ({conexaoInfo['endereco']}:{conexaoInfo['porta']}) se desconectou")
				break

			try:
				resposta = json.loads(conexao.recv(4096).decode("utf8"))
				if resposta == "":
					self.__remover(id, self.conexoes_entrada)
					print(f"\t<Entrada> O host de id {id} ({conexaoInfo['endereco']}:{conexaoInfo['porta']}) se desconectou")
					break
			except:
				self.__remover(id, self.conexoes_entrada)
				print(f"\t<Entrada> O host de id {id} ({conexaoInfo['endereco']}:{conexaoInfo['porta']}) se desconectou")
				break

			self.__atualizarVinculo(id, resposta['vinculo'])

	## Function VisitaNode
	# Visita o node para verificar se ele esta em algum grupo de conexões
	# Um grupo seria alguns nós conectados entre si, porem este não esta 
	# conectado com os outros hosts da rede, mantem assim grupos não conectados entre si
	def __visitaNode(self, id, grupo):
		
		if self.conexoes_entrada[id]["vinculo"] == -1:
			return 

		if self.conexoes_entrada[id]['grupo'] == 0:
			self.conexoes_entrada[id]['grupo'] = grupo
			grupoVisitado = self.__visitaNode(self.conexoes_entrada[id]["vinculo"], grupo)

			if grupoVisitado != grupo:
				self.conexoes_entrada[id]['grupo'] = grupoVisitado

			return grupoVisitado

		return self.conexoes_entrada[id]['grupo']
	## Function IdentificaGrupo
	# Verifica se um nó esta em algum grupo, e coloca o id de grupo
	def __identificaGrupos(self):
		grupo = 1

		for id in list(self.conexoes_entrada.keys()):
			self.conexoes_entrada[id]['grupo'] = 0

		ids = list(self.conexoes_entrada.keys())
		for id in ids:
			if self.conexoes_entrada[id]['grupo'] == 0:
				try:
					self.__visitaNode(id, grupo)
				except:
					pass
				grupo += 1
		
		grupos = {}
		for id in ids:
			if self.conexoes_entrada[id]['grupo'] != 0:
				if grupos.get(self.conexoes_entrada[id]['grupo']) == None:
					grupos[self.conexoes_entrada[id]['grupo']] = []

				grupos[self.conexoes_entrada[id]['grupo']].append(id)

		return grupos

	## Function PrecisamVincular
	# Informa quais host proecisam se vincular para que não hajam grupos desvinculados
	def __precisamVincular(self):
		tamMaiorGrupo = 0
		maiorIdGrupo = None
		vinculos = []
		grupos = self.__identificaGrupos()

		if len(grupos) > 1:
			for idGrupo in list(grupos.keys()):
				tamGrupo = len(grupos[idGrupo])
				if tamMaiorGrupo < tamGrupo:
					tamMaiorGrupo = tamGrupo
					maiorIdGrupo = idGrupo
			for idGrupo in list(grupos.keys()):
				if idGrupo == maiorIdGrupo:
					continue
				vinculos.append((list(grupos[maiorIdGrupo])[-1], list(grupos[idGrupo])[0]))

		return vinculos

	## Function RecebeConexãoEntrada
	# Ele escuta os novos host que querem se conectar
	def __receberConexoesEntrada(self):
		while True:
			if self.isEntrada:
				print("a")
				while not self.conectado:
					continue

				self.entrada = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				self.entrada.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

				if self.endereco_entrada == self.endereco:
					try:
						self.entrada.bind((self.endereco_entrada, self.porta_entrada))
						self.isEntrada = True
						print("\t<Entrada> Esta é a entrada")
					except:
						print("nao é entrada")
						self.isEntrada = False
						continue
				else:
					self.isEntrada = False
					print('checou')
					continue

				self.conexoes_entrada = {}

				conexaoInfo = {
					"entrada": True,
					"endereco": self.endereco,
					"porta": self.porta,
					"vinculo": -1
				}
				self.id = str(self.idNovo)
				self.idNovo += 1
				self.conexoes_entrada[self.id] = conexaoInfo
				print(f"\t<Entrada> O host de id {self.id} ({conexaoInfo['endereco']}:{conexaoInfo['porta']}) se conectou")
				print(f"\nId deste host: {self.id}\n")

				self.entrada.listen(100)

				while True:
					conexao, endereco = self.entrada.accept()
					start_new_thread(self.__manterConexao, (conexao,))

	## Function receberConexoes
	#  @param host
	#  Remove o host da lista de hosts conectados
	def __receberConexoes(self):
		host = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		host.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		while not self.conectado:
			try:
				host.bind((self.endereco, self.porta))
				self.conectado = True
			except:
				self.porta += 100
				continue

		host.listen(100)

		while True:
			conexao, endereco = host.accept()

			while True:
				_resposta = conexao.recv(4096).decode("utf8")
				if _resposta != "":
					break
			resposta = json.loads(_resposta)

			if resposta["tipo"] == "conexao":
				if self.conexoes_entrada.get(resposta['conexao']['id']) != None:
					print(f"O host de id {resposta['conexao']['id']} se conectou aqui")
					start_new_thread(self.__receberMensagem, (resposta['conexao']['id'],))
					self.conexoes[resposta['conexao']['id']] = conexao
					
	## Function LerMensagem
	# Lê a mensagem e envia para aqqueles que este host se conectou
	def lerMensagem(self):
		while True:
			while not self.conectado:
				continue

			conteudo = sys.stdin.readline()

			self.idMensagem = ""
			for i in range(5):
				self.idMensagem += random.choice(string.ascii_letters)

			body = {
				"tipo": "mensagem",
				"mensagem": {
					"id": self.idMensagem,
					"idHost": self.id,
					"endereco": f"{self.endereco}:{self.porta}",
					"conteudo": conteudo.strip('\n')
				}
			}

			self.__repassarMensagem(body)

			sys.stdout.write("<Você> ")
			sys.stdout.write(body["mensagem"]["conteudo"] + '\n')
			sys.stdout.flush()

host = Host(endereco_entrada, porta_entrada, endereco, porta)

host.lerMensagem()

host.hostSocket.close()
entrada.close()
