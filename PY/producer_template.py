# ==============================================================================
# producer_template.py
# Template reutilizável de Producer Apache Kafka — kafka-python
# ------------------------------------------------------------------------------
# COMO USAR:
#   1. Altere apenas a seção "CONFIGURAÇÃO DO PROJETO" abaixo
#   2. Substitua obter_dados_backend() pela sua fonte de dados real
#   3. Ajuste o campo CHAVE_DA_MENSAGEM para o campo que identifica seu evento
# ==============================================================================

import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ==============================================================================
# CONFIGURAÇÃO DO PROJETO — ALTERE APENAS AQUI
# ==============================================================================

KAFKA_BROKER           = "IP_DO_SERVIDOR:9092"       # IP:porta do broker Kafka
TOPICO                 = "nome-do-topico"             # Tópico de destino no Kafka
CHAVE_DA_MENSAGEM      = "user_id"                    # Campo do JSON usado como chave
                                                      # (define em qual partição o evento vai)
                                                      # Exemplos: "user_id", "order_id", "session_id"

CAMINHO_DADOS_ENTRADA  = "backend.services.pedidos"   # Caminho lógico da sua fonte de dados
                                                      # (apenas para documentação e log)
                                                      # Em produção: substitua obter_dados_backend()
                                                      # por um import real desse módulo

CAMINHO_PROCESSAMENTO  = "events.topicos.nome-do-topico"  # Caminho lógico do destino
                                                           # (apenas para documentação e log)

# ==============================================================================
# CONFIGURAÇÃO DE LOG
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ==============================================================================
# FONTE DE DADOS
# Substitua esta função pelo import do seu backend ou API real.
#
# Exemplo de integração com API Flask:
#   from backend.services.pedidos import buscar_pedidos_pendentes
#   def obter_dados_backend():
#       return buscar_pedidos_pendentes()
#
# Exemplo de integração com banco de dados:
#   from backend.db import session
#   from backend.models import Pedido
#   def obter_dados_backend():
#       return [p.to_dict() for p in session.query(Pedido).filter_by(status="novo")]
#
# Exemplo recebendo dado de um endpoint HTTP (webhook):
#   # Nesse caso o producer fica dentro da rota do Flask/FastAPI
#   # e recebe o dado via request.get_json() — veja comentário no main()
# ==============================================================================

def obter_dados_backend() -> list[dict]:
    """
    Retorna uma lista de eventos/dicionários para publicar no Kafka.
    EM PRODUÇÃO: substitua pelo import do seu módulo real.
    AQUI: simulação de dados vindos de um backend.
    """
    log.info(f"Obtendo dados de: {CAMINHO_DADOS_ENTRADA}")

    # ── SIMULAÇÃO — remova em produção ──────────────────────────────────────
    dados_simulados = [
        {
            "user_id":   "u-001",
            "produto":   "notebook",
            "quantidade": 1,
            "valor":     4999.90,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        },
        {
            "user_id":   "u-002",
            "produto":   "teclado",
            "quantidade": 2,
            "valor":     299.90,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        },
    ]
    return dados_simulados
    # ── FIM DA SIMULAÇÃO ────────────────────────────────────────────────────


# ==============================================================================
# PRODUCER
# ==============================================================================

def criar_producer() -> KafkaProducer:
    """
    Cria e retorna uma instância configurada do KafkaProducer.
    Configurações de produção incluídas:
      - acks="all"    → aguarda confirmação de todos os replicas antes de prosseguir
      - retries=5     → tenta reenviar até 5 vezes em caso de falha de rede
      - linger_ms=10  → aguarda 10ms para agrupar mensagens em batch (melhor throughput)
      - compression   → comprime com gzip para economizar banda
    """
    log.info(f"Conectando ao broker: {KAFKA_BROKER}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,

        # Serialização: converte dict Python → bytes JSON
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,

        # Confiabilidade
        acks="all",           # espera todos os replicas confirmarem
        retries=5,            # tentativas em caso de falha
        retry_backoff_ms=500, # espera 500ms entre tentativas

        # Performance
        linger_ms=10,               # agrupa mensagens por 10ms antes de enviar
        batch_size=16384,           # tamanho máximo do batch em bytes (16KB)
        compression_type="gzip",    # comprime os dados antes de enviar

        # Timeout
        request_timeout_ms=30000,   # 30 segundos de timeout por requisição
    )
    log.info("Producer conectado com sucesso.")
    return producer


def publicar_evento(producer: KafkaProducer, dados: dict) -> bool:
    """
    Serializa um dicionário Python em JSON e publica no tópico Kafka.

    Parâmetros:
        producer  — instância do KafkaProducer (criada por criar_producer())
        dados     — dicionário com os dados do evento

    Retorna:
        True se publicado com sucesso, False caso contrário.

    A CHAVE define em qual partição o evento vai:
        - Mesma chave = mesma partição = ordem garantida para aquele registro
        - Exemplo: todos os eventos do user_id "u-001" sempre vão para a mesma partição
    """
    try:
        # Extrai a chave do dicionário de dados
        chave = str(dados.get(CHAVE_DA_MENSAGEM, "sem-chave"))

        # Envia para o Kafka e aguarda confirmação (timeout: 10s)
        future = producer.send(topic=TOPICO, key=chave, value=dados)
        metadata = future.get(timeout=10)  # bloqueia até o broker confirmar

        log.info(
            f"✅ Publicado | tópico={metadata.topic} | "
            f"partição={metadata.partition} | "
            f"offset={metadata.offset} | "
            f"chave={chave}"
        )
        return True

    except KafkaError as e:
        log.error(f"❌ Erro ao publicar evento: {e} | dados={dados}")
        return False


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    """
    Fluxo principal do producer:
      1. Cria o producer conectado ao broker
      2. Obtém os dados do backend (substitua por sua fonte real)
      3. Publica cada evento no tópico
      4. Fecha a conexão corretamente

    INTEGRAÇÃO COM FLASK/FASTAPI (webhook):
      Em vez de rodar main() em loop, coloque publicar_evento() dentro da sua rota:

        @app.route("/pedidos", methods=["POST"])
        def receber_pedido():
            dados = request.get_json()
            producer = criar_producer()
            publicar_evento(producer, dados)
            producer.flush()
            return {"status": "recebido"}, 202
    """
    log.info(f"Producer iniciado | broker={KAFKA_BROKER} | tópico={TOPICO}")
    log.info(f"Fonte de dados   : {CAMINHO_DADOS_ENTRADA}")
    log.info(f"Destino lógico   : {CAMINHO_PROCESSAMENTO}")

    producer = criar_producer()

    try:
        lista_de_eventos = obter_dados_backend()
        log.info(f"{len(lista_de_eventos)} evento(s) obtido(s) para publicar.")

        sucesso = 0
        falha   = 0

        for evento in lista_de_eventos:
            if publicar_evento(producer, evento):
                sucesso += 1
            else:
                falha += 1

        # Garante que todos os eventos pendentes foram enviados antes de fechar
        producer.flush()
        log.info(f"Resultado: {sucesso} publicado(s), {falha} falha(s).")

    except Exception as e:
        log.critical(f"Erro inesperado no producer: {e}", exc_info=True)

    finally:
        producer.close()
        log.info("Producer encerrado.")


if __name__ == "__main__":
    main()


# ==============================================================================
# GUIA DE INTEGRAÇÃO
# ==============================================================================
#
# VARIÁVEIS QUE VOCÊ PRECISA MUDAR:
# ─────────────────────────────────
#   KAFKA_BROKER          → IP:porta do seu servidor Kafka
#                           Ex: "192.168.1.10:9092"
#
#   TOPICO                → nome do tópico que você criou com kafka-topics --create
#                           Ex: "pedidos-criados", "usuarios-novos"
#
#   CHAVE_DA_MENSAGEM     → campo do seu dicionário que define a partição
#                           Use o campo que identifica o "dono" do evento
#                           Ex: "user_id", "order_id", "session_id"
#
#   CAMINHO_DADOS_ENTRADA → documentação do módulo de onde vêm os dados
#                           (não afeta o comportamento, só os logs)
#
#   CAMINHO_PROCESSAMENTO → documentação do destino lógico dos eventos
#                           (não afeta o comportamento, só os logs)
#
# ONDE INTEGRAR COM SUA API BACKEND:
# ────────────────────────────────────
#   Opção A — Producer chamado por uma rota HTTP (Flask/FastAPI):
#     Coloque criar_producer() fora da rota (instância global ou singleton)
#     Chame publicar_evento(producer, dados) dentro da rota
#
#   Opção B — Producer em loop lendo de um banco:
#     Substitua obter_dados_backend() por uma query ao banco
#     Rode main() como um worker/serviço separado
#
#   Opção C — Producer em módulo importado:
#     from producer_template import criar_producer, publicar_evento
#     Use as funções diretamente no seu código
#
# ESTRUTURA DE MÓDULOS SUGERIDA:
# ────────────────────────────────
#   backend/
#   ├── services/
#   │   └── pedidos.py          ← substitui obter_dados_backend()
#   └── app.py                  ← chama publicar_evento() nas rotas
#   events/
#   └── producer_template.py    ← este arquivo
# ==============================================================================
