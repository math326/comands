# ==============================================================================
# consumer_template.py
# Template reutilizável de Consumer Apache Kafka — kafka-python
# ------------------------------------------------------------------------------
# COMO USAR:
#   1. Altere apenas a seção "CONFIGURAÇÃO DO PROJETO" abaixo
#   2. Coloque sua lógica de negócio dentro de processar_evento()
#   3. Substitua enviar_para_servico() pela chamada ao seu microserviço real
# ==============================================================================

import json
import logging
import time
import requests                  # pip install requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ==============================================================================
# CONFIGURAÇÃO DO PROJETO — ALTERE APENAS AQUI
# ==============================================================================

KAFKA_BROKER           = "IP_DO_SERVIDOR:9092"       # IP:porta do broker Kafka
TOPICO                 = "nome-do-topico"             # Tópico que este consumer vai ler
GRUPO_CONSUMER         = "grupo-nome-do-servico"      # ID do grupo de consumers
                                                      # Consumers do mesmo grupo dividem
                                                      # as partições entre si (load balance)
                                                      # Consumers de grupos diferentes recebem
                                                      # todos os mesmos eventos (broadcast)

CAMINHO_DADOS_ENTRADA  = "events.topicos.nome-do-topico"  # Caminho lógico da fonte
                                                           # (apenas para log/documentação)

CAMINHO_PROCESSAMENTO  = "services.nome-do-servico"       # Caminho lógico do destino
                                                           # (apenas para log/documentação)

# Endereço do microserviço ou API que receberá o dado processado
# Substitua pelo IP/URL real do seu serviço de destino
URL_SERVICO_DESTINO    = "http://IP_DO_SERVICO_DESTINO:8000/endpoint"

# ==============================================================================
# CONFIGURAÇÃO DE LOG
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ==============================================================================
# LÓGICA DE NEGÓCIO
# Esta é a função principal que você customiza para cada projeto.
# Recebe o evento bruto (dicionário Python) lido da partição Kafka.
#
# Exemplos do que pode estar aqui:
#   - Validar campos obrigatórios
#   - Calcular valores derivados
#   - Consultar banco de dados
#   - Chamar uma API externa
#   - Publicar em outro tópico Kafka (Consumer + Producer combinados)
#   - Salvar em banco, cache, arquivo, etc.
# ==============================================================================

def processar_evento(evento: dict) -> dict:
    """
    Contém a lógica de negócio do serviço.
    Recebe o evento lido do Kafka e retorna o resultado processado.

    Parâmetros:
        evento — dicionário Python desserializado do JSON do Kafka

    Retorna:
        resultado — dicionário com os dados processados,
                    que será passado para enviar_para_servico()

    EM PRODUÇÃO: substitua o conteúdo desta função pela sua lógica real.
    Você pode também importar funções de outros módulos:

        from services.pagamentos import validar_pagamento
        from db.repository import salvar_pedido

        resultado = validar_pagamento(evento)
        salvar_pedido(resultado)
        return resultado
    """
    log.info(f"Processando evento: {evento}")

    # ── SIMULAÇÃO DA LÓGICA DE NEGÓCIO — substitua em produção ─────────────

    # Exemplo: valida campos obrigatórios
    campos_obrigatorios = ["user_id", "produto", "valor"]
    for campo in campos_obrigatorios:
        if campo not in evento:
            raise ValueError(f"Campo obrigatório ausente no evento: '{campo}'")

    # Exemplo: transforma os dados (sua lógica real vai aqui)
    resultado = {
        "user_id":          evento["user_id"],
        "produto":          evento["produto"],
        "valor_original":   evento["valor"],
        "valor_com_taxa":   round(evento["valor"] * 1.05, 2),  # aplica taxa de 5%
        "status":           "aprovado",
        "processado_em":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    log.info(f"Evento processado com sucesso: {resultado}")
    return resultado

    # ── FIM DA SIMULAÇÃO ────────────────────────────────────────────────────


# ==============================================================================
# ENVIO PARA OUTRO SERVIÇO
# Esta função recebe o resultado de processar_evento() e o envia
# para o próximo passo do seu sistema.
#
# Exemplos do que pode estar aqui:
#   - POST para uma API REST (implementado abaixo como padrão)
#   - Inserção em banco de dados
#   - Publicação em outro tópico Kafka
#   - Gravação em arquivo/S3/storage
#   - Envio para fila SQS, RabbitMQ, etc.
# ==============================================================================

def enviar_para_servico(resultado: dict) -> bool:
    """
    Envia o evento processado para o próximo serviço/módulo do sistema.

    Parâmetros:
        resultado — dicionário retornado por processar_evento()

    Retorna:
        True se entregue com sucesso, False caso contrário.

    EM PRODUÇÃO: substitua o corpo desta função pela sua integração real.

    Exemplos de integração:

    ── Banco de dados (SQLAlchemy): ────────────────────────────────────────────
        from db.session import Session
        from db.models import Pedido
        with Session() as session:
            session.add(Pedido(**resultado))
            session.commit()
        return True

    ── Outro tópico Kafka (Consumer + Producer): ────────────────────────────────
        from producer_template import criar_producer, publicar_evento
        producer = criar_producer()   # use instância global em produção
        return publicar_evento(producer, resultado)

    ── Redis: ───────────────────────────────────────────────────────────────────
        import redis
        r = redis.Redis(host="localhost", port=6379)
        r.set(resultado["user_id"], json.dumps(resultado))
        return True
    """
    log.info(f"Enviando resultado para: {CAMINHO_PROCESSAMENTO} ({URL_SERVICO_DESTINO})")

    # ── PADRÃO: POST HTTP para o microserviço de destino ────────────────────
    try:
        resposta = requests.post(
            URL_SERVICO_DESTINO,
            json=resultado,
            headers={"Content-Type": "application/json"},
            timeout=10   # 10 segundos de timeout
        )
        resposta.raise_for_status()   # lança exceção se status 4xx ou 5xx

        log.info(f"✅ Entregue ao serviço | status_http={resposta.status_code}")
        return True

    except requests.exceptions.ConnectionError:
        log.error(f"❌ Serviço indisponível: {URL_SERVICO_DESTINO}")
        return False

    except requests.exceptions.Timeout:
        log.error(f"❌ Timeout ao chamar: {URL_SERVICO_DESTINO}")
        return False

    except requests.exceptions.HTTPError as e:
        log.error(f"❌ Erro HTTP do serviço: {e}")
        return False
    # ── FIM DO PADRÃO HTTP ───────────────────────────────────────────────────


# ==============================================================================
# CONSUMER
# ==============================================================================

def criar_consumer() -> KafkaConsumer:
    """
    Cria e retorna uma instância configurada do KafkaConsumer.
    Configurações de produção incluídas:
      - auto_offset_reset="earliest"  → se o consumer nunca leu este tópico,
                                        começa do primeiro evento disponível
                                        Use "latest" para ler só eventos novos
      - enable_auto_commit=False      → offset confirmado manualmente após
                                        processar_evento() e enviar_para_servico()
                                        (garante que nenhum evento seja perdido)
      - max_poll_records=10           → processa até 10 mensagens por batch
    """
    log.info(f"Conectando ao broker: {KAFKA_BROKER}")
    log.info(f"Tópico: {TOPICO} | Grupo: {GRUPO_CONSUMER}")

    consumer = KafkaConsumer(
        TOPICO,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GRUPO_CONSUMER,

        # Offset: de onde começar a ler
        auto_offset_reset="earliest",  # "earliest" = do início | "latest" = só novos

        # Commit manual: só confirma offset depois de processar com sucesso
        enable_auto_commit=False,

        # Desserialização: bytes → dict Python via JSON
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,

        # Performance
        max_poll_records=10,            # máximo de mensagens por poll
        fetch_max_bytes=1048576,        # máximo de bytes por fetch (1MB)
        session_timeout_ms=30000,       # 30s — tempo para o broker considerar o consumer morto
        heartbeat_interval_ms=10000,    # 10s — frequência do heartbeat para o broker
    )
    log.info("Consumer conectado e aguardando eventos.")
    return consumer


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """
    Loop principal do consumer:
      1. Conecta ao broker e começa a escutar o tópico
      2. Para cada mensagem recebida:
         a. Desserializa o JSON para dict Python
         b. Chama processar_evento() com sua lógica de negócio
         c. Chama enviar_para_servico() para entregar o resultado
         d. Confirma o offset (informa ao Kafka que a mensagem foi processada)
      3. Em caso de erro, registra o log mas NÃO confirma o offset
         (o Kafka reentregará a mensagem na próxima sessão)

    INTEGRAÇÃO COM FLASK/FASTAPI (consumer em background):
      Em vez de rodar main() diretamente, rode em thread separada:

        import threading
        from consumer_template import main as consumer_main

        thread = threading.Thread(target=consumer_main, daemon=True)
        thread.start()

        app.run(host="0.0.0.0", port=4000)
    """
    log.info("=" * 60)
    log.info("Consumer iniciado")
    log.info(f"  Broker  : {KAFKA_BROKER}")
    log.info(f"  Tópico  : {TOPICO}")
    log.info(f"  Grupo   : {GRUPO_CONSUMER}")
    log.info(f"  Fonte   : {CAMINHO_DADOS_ENTRADA}")
    log.info(f"  Destino : {CAMINHO_PROCESSAMENTO}")
    log.info("=" * 60)

    consumer = criar_consumer()
    total_processados = 0
    total_erros       = 0

    try:
        # Loop infinito — fica escutando o tópico indefinidamente
        for mensagem in consumer:

            log.info(
                f"📨 Mensagem recebida | "
                f"partição={mensagem.partition} | "
                f"offset={mensagem.offset} | "
                f"chave={mensagem.key}"
            )

            try:
                # ── Passo 1: processar o evento (sua lógica de negócio) ────
                resultado = processar_evento(mensagem.value)

                # ── Passo 2: entregar o resultado ao próximo serviço ───────
                entregue = enviar_para_servico(resultado)

                if entregue:
                    # ── Passo 3: confirma offset — só após sucesso ─────────
                    # Sem isso, o Kafka reentregará a mensagem ao reiniciar
                    consumer.commit()
                    total_processados += 1
                    log.info(f"✅ Mensagem concluída | total={total_processados}")
                else:
                    # Não confirma o offset — será reprocessado depois
                    total_erros += 1
                    log.warning(
                        f"⚠️ Entrega falhou — offset NÃO confirmado | "
                        f"offset={mensagem.offset} | será reprocessado"
                    )

            except ValueError as e:
                # Erro de validação — dado inválido, não tente reprocessar
                total_erros += 1
                log.error(f"❌ Dado inválido, descartando | erro={e} | dado={mensagem.value}")
                consumer.commit()   # confirma para não ficar em loop com dado inválido

            except Exception as e:
                # Erro inesperado — não confirma o offset para reprocessar
                total_erros += 1
                log.error(f"❌ Erro ao processar | offset={mensagem.offset} | erro={e}", exc_info=True)

    except KeyboardInterrupt:
        log.info("Consumer interrompido manualmente (Ctrl+C).")

    except KafkaError as e:
        log.critical(f"Erro crítico no Kafka: {e}", exc_info=True)

    finally:
        consumer.close()
        log.info(f"Consumer encerrado | processados={total_processados} | erros={total_erros}")


if __name__ == "__main__":
    main()


# ==============================================================================
# GUIA DE INTEGRAÇÃO
# ==============================================================================
#
# VARIÁVEIS QUE VOCÊ PRECISA MUDAR:
# ─────────────────────────────────
#   KAFKA_BROKER           → IP:porta do seu servidor Kafka
#                            Ex: "192.168.1.10:9092"
#
#   TOPICO                 → nome do tópico que este consumer vai ler
#                            Ex: "pedidos-criados", "usuarios-novos"
#
#   GRUPO_CONSUMER         → ID do grupo de consumers
#                            Use um nome diferente por serviço
#                            Ex: "grupo-pagamentos", "grupo-notificacoes"
#                            IMPORTANTE: dois consumers com o mesmo grupo
#                            dividem as partições (load balance)
#                            Grupos diferentes recebem os mesmos eventos
#
#   URL_SERVICO_DESTINO    → endereço HTTP do microserviço que receberá
#                            o dado processado
#                            Ex: "http://192.168.1.20:8000/resultados"
#
#   CAMINHO_DADOS_ENTRADA  → documentação do tópico de origem (só log)
#   CAMINHO_PROCESSAMENTO  → documentação do serviço de destino (só log)
#
# ONDE COLOCAR SUA LÓGICA DE NEGÓCIO:
# ─────────────────────────────────────
#   Função: processar_evento(evento: dict) -> dict
#   Exemplos do que colocar:
#     - Validações de campos
#     - Cálculos e transformações
#     - Consultas ao banco de dados
#     - Chamadas a APIs externas
#
# ONDE INTEGRAR COM MICROSERVIÇOS:
# ──────────────────────────────────
#   Função: enviar_para_servico(resultado: dict) -> bool
#   Troque o corpo desta função por:
#     - Insert no banco de dados (SQLAlchemy, psycopg2)
#     - Publicação em outro tópico Kafka (importe producer_template)
#     - Gravação em Redis, S3, arquivo
#     - POST para outra API REST
#
# ESTRUTURA DE MÓDULOS SUGERIDA:
# ────────────────────────────────
#   services/
#   ├── consumer_template.py    ← este arquivo
#   ├── producer_template.py    ← para Consumer+Producer combinado
#   └── nome_do_servico.py      ← sua lógica importada em processar_evento()
#   backend/
#   └── app.py                  ← chama consumer em thread se necessário
#
# RODAR EM PRODUÇÃO:
# ───────────────────
#   python consumer_template.py
#   — ou —
#   Dockerize e suba como um container separado por serviço
# ==============================================================================
