package com.producer;

// =============================================================
// EventoProducer.java — Serviço Producer Kafka (Spring Boot 3)
//
// Este arquivo contém TODAS as classes do serviço em um único
// lugar para facilitar o entendimento e a portabilidade.
//
// Estrutura:
//   1. EventoProducerApplication — entry point Spring Boot
//   2. Evento                    — modelo de dados (DTO)
//   3. EventoController          — endpoints REST
//   4. EventoProducerService     — lógica de negócio + envio Kafka
//   5. KafkaProducerConfig       — configuração do producer Kafka
// =============================================================

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// =============================================================
// 1. ENTRY POINT — inicializa a aplicação Spring Boot
// =============================================================

@SpringBootApplication
public class EventoProducer {

    public static void main(String[] args) {
        SpringApplication.run(EventoProducer.class, args);
    }
}

// =============================================================
// 2. MODELO — representa o evento trafegado no Kafka
//
// CAMPOS PADRÃO: não remova — são esperados pelo contrato base.
// CAMPOS CUSTOMIZADOS: adicione novos campos na seção indicada.
// Todos os campos são opcionais no request (exceto os de negócio
// que você validar em validarEvento()).
// =============================================================

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
class Evento {

    // --- Campos de controle (gerados automaticamente se ausentes) ---

    /** ID único da transação. Gerado via UUID v4 se não informado. */
    private String transacao_id;

    /** Timestamp ISO-8601 do momento do evento. Preenchido automaticamente. */
    private String timestamp;

    // --- Campos de negócio padrão ---

    /** Conta de origem da operação. */
    private String conta_origem;

    /** Conta de destino da operação. */
    private String conta_destino;

    /** Valor monetário da operação. */
    private BigDecimal valor;

    /**
     * Tipo do evento.
     * Exemplos: "PIX", "TED", "DOC", "BOLETO"
     */
    private String tipo;

    /**
     * Status atual do evento.
     * Exemplos: "PENDENTE", "PROCESSANDO", "CONCLUIDO", "ERRO"
     */
    private String status;

    // =========================================================
    // ADICIONE AQUI OS CAMPOS CUSTOMIZADOS DO SEU PROJETO
    // Exemplo:
    //   private String canal;          // "APP", "WEB", "API"
    //   private String moeda;          // "BRL", "USD"
    //   private String descricao;
    //   private Map<String, Object> metadata;
    // =========================================================
}

// =============================================================
// 3. CONTROLLER — expõe os endpoints REST da aplicação
// =============================================================

@Slf4j
@RestController
@RequestMapping("/api")
class EventoController {

    private final EventoProducerService producerService;

    EventoController(EventoProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * POST /api/eventos
     *
     * Recebe um evento do frontend ou de outro backend e o publica
     * no Kafka de forma assíncrona. Retorna 202 Accepted imediatamente
     * — o ACK do Kafka é tratado internamente via callback.
     *
     * Body: JSON com os campos do modelo Evento.
     * Campos ausentes são preenchidos automaticamente (transacao_id,
     * timestamp) ou validados pelo service (conta_origem, valor, etc.).
     */
    @PostMapping("/eventos")
    public ResponseEntity<Map<String, Object>> receberEvento(@RequestBody Evento evento) {
        log.info("Evento recebido via HTTP: transacao_id={}", evento.getTransacao_id());

        try {
            producerService.publicarEvento(evento);

            Map<String, Object> resposta = new HashMap<>();
            resposta.put("status", "aceito");
            resposta.put("transacao_id", evento.getTransacao_id());
            resposta.put("mensagem", "Evento enfileirado para publicação no Kafka");

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(resposta);

        } catch (IllegalArgumentException e) {
            // Erro de validação de negócio — retorna 400
            log.warn("Evento rejeitado por validação: {}", e.getMessage());

            Map<String, Object> erro = new HashMap<>();
            erro.put("status", "rejeitado");
            erro.put("motivo", e.getMessage());

            return ResponseEntity.badRequest().body(erro);

        } catch (Exception e) {
            // Erro inesperado — retorna 500
            log.error("Falha ao processar evento: {}", e.getMessage(), e);

            Map<String, Object> erro = new HashMap<>();
            erro.put("status", "erro");
            erro.put("motivo", "Falha interna ao publicar o evento");

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(erro);
        }
    }

    /**
     * GET /api/health
     *
     * Endpoint de health check — usado por load balancers, Kubernetes
     * liveness/readiness probes e ferramentas de monitoramento.
     * Retorna 200 OK com informações básicas do serviço.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> resposta = new HashMap<>();
        resposta.put("status", "UP");
        resposta.put("servico", "kafka-evento-producer");
        resposta.put("versao", "1.0.0");
        return ResponseEntity.ok(resposta);
    }
}

// =============================================================
// 4. SERVICE — lógica de negócio e publicação no Kafka
// =============================================================

@Slf4j
@Service
class EventoProducerService {

    // Valores injetados do application.properties
    @Value("${kafka.topico}")
    private String topico;

    @Value("${kafka.chave-campo}")
    private String chaveCampo;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    EventoProducerService(KafkaTemplate<String, String> kafkaTemplate,
                          ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper  = objectMapper;
    }

    /**
     * Ponto de entrada principal: prepara e publica o evento no Kafka.
     * Chamado pelo controller após receber o request HTTP.
     */
    public void publicarEvento(Evento evento) throws JsonProcessingException {

        // Preenche campos automáticos antes da validação
        preencherCamposAutomaticos(evento);

        // Valida as regras de negócio — lança IllegalArgumentException se inválido
        validarEvento(evento);

        // Extrai a chave de particionamento configurada
        String chave = extrairChave(evento);

        // Serializa o evento para JSON
        String payload = objectMapper.writeValueAsString(evento);

        // Publica no Kafka com callback assíncrono
        enviarComCallback(topico, chave, payload, evento.getTransacao_id());
    }

    // ---------------------------------------------------------
    // VALIDAÇÕES DE NEGÓCIO — ALTERE AQUI
    //
    // Adicione ou remova validações conforme as regras do seu
    // projeto. Lance IllegalArgumentException com mensagem clara
    // para que o controller devolva 400 ao chamador.
    // ---------------------------------------------------------
    private void validarEvento(Evento evento) {

        // Exemplo: conta_origem obrigatória
        if (!StringUtils.hasText(evento.getConta_origem())) {
            throw new IllegalArgumentException("Campo 'conta_origem' é obrigatório");
        }

        // Exemplo: conta_destino obrigatória
        if (!StringUtils.hasText(evento.getConta_destino())) {
            throw new IllegalArgumentException("Campo 'conta_destino' é obrigatório");
        }

        // Exemplo: valor deve ser positivo
        if (evento.getValor() == null || evento.getValor().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Campo 'valor' deve ser maior que zero");
        }

        // Exemplo: tipo obrigatório
        if (!StringUtils.hasText(evento.getTipo())) {
            throw new IllegalArgumentException("Campo 'tipo' é obrigatório");
        }

        // =====================================================
        // ADICIONE SUAS VALIDAÇÕES CUSTOMIZADAS AQUI
        // Exemplo:
        //   if (evento.getValor().compareTo(new BigDecimal("50000")) > 0) {
        //       throw new IllegalArgumentException("Valor acima do limite permitido");
        //   }
        // =====================================================
    }

    /**
     * Preenche campos gerados automaticamente caso não venham no request.
     * Não é necessário alterar este método.
     */
    private void preencherCamposAutomaticos(Evento evento) {

        // Gera transacao_id único se o chamador não informou
        if (!StringUtils.hasText(evento.getTransacao_id())) {
            evento.setTransacao_id(UUID.randomUUID().toString());
            log.debug("transacao_id gerado automaticamente: {}", evento.getTransacao_id());
        }

        // Preenche timestamp com o momento atual em UTC
        if (!StringUtils.hasText(evento.getTimestamp())) {
            evento.setTimestamp(Instant.now().toString());
        }

        // Define status padrão se não informado
        if (!StringUtils.hasText(evento.getStatus())) {
            evento.setStatus("PENDENTE");
        }
    }

    /**
     * Extrai o valor do campo configurado em kafka.chave-campo para usar
     * como chave de particionamento. Se o campo não existir no Evento,
     * usa o transacao_id como fallback (distribui uniformemente).
     */
    private String extrairChave(Evento evento) {
        try {
            // Converte o Evento para Map e busca o campo configurado
            @SuppressWarnings("unchecked")
            Map<String, Object> campos = objectMapper.convertValue(evento, Map.class);
            Object valorCampo = campos.get(chaveCampo);

            if (valorCampo != null && StringUtils.hasText(valorCampo.toString())) {
                return valorCampo.toString();
            }
        } catch (Exception e) {
            log.warn("Não foi possível extrair a chave '{}', usando transacao_id como fallback",
                     chaveCampo);
        }

        return evento.getTransacao_id();
    }

    /**
     * Envia a mensagem ao Kafka e registra o resultado via callback assíncrono.
     * O thread HTTP retorna imediatamente — o ACK é processado em background.
     *
     * Garantias configuradas:
     *   - acks=all       → aguarda confirmação de todas as réplicas
     *   - retries=10     → tenta novamente em caso de falha transiente
     *   - idempotência   → evita duplicatas mesmo com retries
     */
    private void enviarComCallback(String topico, String chave,
                                   String payload, String transacaoId) {

        ProducerRecord<String, String> record = new ProducerRecord<>(topico, chave, payload);

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                // Falha após todos os retries — registra para alertas/DLQ
                log.error("[KAFKA] FALHA ao publicar | transacao_id={} | erro={}",
                          transacaoId, ex.getMessage(), ex);
                // =====================================================
                // ADICIONE AQUI: envio para DLQ, alerta, métrica, etc.
                // =====================================================
            } else {
                RecordMetadata metadata = result.getRecordMetadata();
                log.info("[KAFKA] Publicado | transacao_id={} | tópico={} | partição={} | offset={}",
                         transacaoId,
                         metadata.topic(),
                         metadata.partition(),
                         metadata.offset());
            }
        });
    }
}

// =============================================================
// 5. CONFIGURAÇÃO DO PRODUCER KAFKA
//
// Todas as propriedades são lidas do application.properties.
// Não há valores hardcoded aqui.
// =============================================================

@Configuration
class KafkaProducerConfig {

    // Lidos de application.properties — seção "ALTERE APENAS ESTA SEÇÃO"
    @Value("${kafka.broker}")
    private String broker;

    /**
     * Fábrica de producers com as configurações de produção.
     *
     * Configurações de confiabilidade:
     *   enable.idempotence=true → garante exactly-once por partição
     *   acks=all                → confirma em todas as réplicas in-sync
     *   retries=10              → tenta novamente em falhas transientes
     *   max.in.flight.requests.per.connection=5 → máximo seguro com idempotência
     *
     * Configurações de performance:
     *   compression.type=gzip  → reduz uso de rede (CPU vs. latência)
     *   linger.ms=5            → aguarda 5ms para agrupar mensagens em batch
     *   batch.size=16384       → tamanho máximo do batch (16 KB)
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();

        // Conexão
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

        // Serialização — chave e valor como texto UTF-8
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Confiabilidade
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,                    true);
        config.put(ProducerConfig.ACKS_CONFIG,                                  "all");
        config.put(ProducerConfig.RETRIES_CONFIG,                               10);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,        5);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,                   120_000); // 2 min

        // Performance
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        config.put(ProducerConfig.LINGER_MS_CONFIG,        5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG,       16_384);

        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * KafkaTemplate é o bean principal usado pelo EventoProducerService
     * para enviar mensagens. Utiliza a ProducerFactory acima.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
