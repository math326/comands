package com.example.consumer;

// ============================================================
// IMPORTS — não alterar
// ============================================================
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


// ==============================================================
// APLICAÇÃO PRINCIPAL — ponto de entrada Spring Boot
// ==============================================================
@SpringBootApplication
class EventoConsumerApp {
    public static void main(String[] args) {
        SpringApplication.run(EventoConsumerApp.class, args);
    }
}


// ==============================================================
// MODEL: Evento
// Deve ser idêntico ao modelo enviado pelo Produtor.
// Adicione ou remova campos conforme o contrato de mensagens.
// ==============================================================
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true) // tolera campos extras sem quebrar a desserialização
class Evento {

    /** Identificador único da mensagem — usado para rastrear o resultado no polling */
    private String id;

    // *** ALTERE OS CAMPOS ABAIXO PARA CORRESPONDER AO SEU PRODUTOR ***

    private String tipo;
    private BigDecimal valor;
    private String timestamp;
    private Map<String, Object> dados;

    // *** FIM DOS CAMPOS DO EVENTO ***
}


// ==============================================================
// MODEL: ResultadoEvento
// Representa o resultado após o processamento do Evento.
// É armazenado no ResultadoStore e retornado ao frontend via polling.
// ==============================================================
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
class ResultadoEvento {

    /** Mesmo ID do Evento recebido — chave de lookup para o frontend */
    private String id;

    /** Status do processamento: ex. "PROCESSADO", "ERRO", "PENDENTE" */
    private String status;

    /** Mensagem descritiva para exibição */
    private String mensagem;

    /** Valor monetário processado (espelha o valor do Evento) */
    private BigDecimal valor;

    /** Timestamp ISO-8601 de quando o processamento foi concluído */
    private String processado_em;

    // ==============================================================
    // *** ADICIONE AQUI OS CAMPOS DE RESULTADO DO SEU PROJETO ***
    // Exemplos:
    //
    // private String codigoAutorizacao;
    // private String clienteId;
    // private String descricaoProduto;
    // private String urlComprovante;
    // private Map<String, Object> detalhes;
    //
    // ==============================================================
}


// ==============================================================
// SERVICE: ResultadoStore
// Armazena os resultados processados em memória (ConcurrentHashMap).
// O frontend faz polling em GET /api/resultado/{id} até encontrar o resultado.
//
// PARA SUBSTITUIR POR REDIS:
// ==============================================================
// 1. Adicione no pom.xml:
//      <dependency>
//          <groupId>org.springframework.boot</groupId>
//          <artifactId>spring-boot-starter-data-redis</artifactId>
//      </dependency>
//
// 2. Adicione no application.properties:
//      spring.data.redis.host=localhost
//      spring.data.redis.port=6379
//
// 3. Configure o RedisTemplate com o serializer correto:
//      @Bean
//      public RedisTemplate<String, ResultadoEvento> redisTemplate(RedisConnectionFactory factory) {
//          RedisTemplate<String, ResultadoEvento> template = new RedisTemplate<>();
//          template.setConnectionFactory(factory);
//          template.setKeySerializer(new StringRedisSerializer());
//          template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
//          return template;
//      }
//
// 4. Substitua o corpo desta classe por:
//      @Autowired private RedisTemplate<String, ResultadoEvento> redisTemplate;
//      private static final Duration TTL = Duration.ofMinutes(30);
//
//      public void salvar(ResultadoEvento r)          { redisTemplate.opsForValue().set(r.getId(), r, TTL); }
//      public Optional<ResultadoEvento> buscar(String id) { return Optional.ofNullable(redisTemplate.opsForValue().get(id)); }
//      public void remover(String id)                 { redisTemplate.delete(id); }
// ==============================================================
@Service
class ResultadoStore {

    private final ConcurrentHashMap<String, ResultadoEvento> store = new ConcurrentHashMap<>();

    public void salvar(ResultadoEvento resultado) {
        store.put(resultado.getId(), resultado);
    }

    public Optional<ResultadoEvento> buscar(String id) {
        return Optional.ofNullable(store.get(id));
    }

    public void remover(String id) {
        store.remove(id);
    }
}


// ==============================================================
// CONTROLLER: ResultadoController
// Endpoints REST consumidos pelo frontend para polling de resultados.
//
// Fluxo de polling:
//   1. Frontend envia evento → aguarda processamento
//   2. Frontend faz GET /api/resultado/{id} em loop até receber 200
//   3. Frontend exibe o resultado e chama DELETE /api/resultado/{id}
// ==============================================================
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
class ResultadoController {

    private final ResultadoStore resultadoStore;

    /**
     * Frontend consulta aqui enquanto aguarda o processamento.
     * Retorna 404 enquanto o resultado ainda não estiver disponível.
     * Retorna 200 com o ResultadoEvento quando pronto.
     */
    @GetMapping("/resultado/{id}")
    public ResponseEntity<ResultadoEvento> buscarResultado(@PathVariable String id) {
        return resultadoStore.buscar(id)
                .map(resultado -> {
                    log.info("Resultado encontrado para id={}", id);
                    return ResponseEntity.ok(resultado);
                })
                .orElseGet(() -> {
                    log.debug("Resultado ainda não disponível para id={}", id);
                    return ResponseEntity.notFound().build();
                });
    }

    /**
     * Frontend chama após exibir o resultado para liberar memória.
     * Sempre retorna 204 (mesmo que o id não exista).
     */
    @DeleteMapping("/resultado/{id}")
    public ResponseEntity<Void> deletarResultado(@PathVariable String id) {
        resultadoStore.remover(id);
        log.info("Resultado removido para id={}", id);
        return ResponseEntity.noContent().build();
    }

    /**
     * Health check — usado por load balancer, k8s liveness probe, etc.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "kafka-consumer",
                "timestamp", Instant.now().toString()
        ));
    }
}


// ==============================================================
// SERVICE: EventoConsumerService
// Contém a lógica de negócio do consumidor.
// Este é o principal arquivo a ser modificado por projeto.
// ==============================================================
@Service
@RequiredArgsConstructor
@Slf4j
class EventoConsumerService {

    @Value("${kafka.destino.url:}")
    private String destinoUrl;

    private final ResultadoStore resultadoStore;
    private final RestTemplate restTemplate;

    // ==============================================================
    // *** PONTO CENTRAL DE NEGÓCIO — IMPLEMENTE AQUI ***
    //
    // Este método é chamado para cada mensagem recebida do Kafka.
    // Implemente aqui toda a lógica de processamento do evento:
    //   - Validações de campos obrigatórios
    //   - Consultas ao banco de dados
    //   - Chamadas a APIs externas / outros microsserviços
    //   - Cálculos, transformações de dados
    //   - Atualização de status de pedidos / transações
    //
    // Lance IllegalArgumentException para dados inválidos
    // (não vai para retry — vai direto para a DLT).
    // Lance qualquer outra RuntimeException para erros transientes
    // (vai para retry e depois para a DLT se esgotar tentativas).
    // ==============================================================
    public ResultadoEvento processarEvento(Evento evento) {
        log.info("Iniciando processamento: id={}, tipo={}", evento.getId(), evento.getTipo());

        // *** IMPLEMENTE A LÓGICA DE NEGÓCIO AQUI ***
        // Exemplo de validação:
        // if (evento.getValor() == null || evento.getValor().compareTo(BigDecimal.ZERO) <= 0) {
        //     throw new IllegalArgumentException("Valor inválido: " + evento.getValor());
        // }
        //
        // Exemplo de consulta a banco:
        // Cliente cliente = clienteRepository.findById(evento.getClienteId())
        //     .orElseThrow(() -> new IllegalArgumentException("Cliente não encontrado"));
        //
        // Exemplo de chamada a API externa:
        // AutorizacaoResponse auth = autorizacaoClient.autorizar(evento);

        // Construa o ResultadoEvento com base na sua lógica
        ResultadoEvento resultado = ResultadoEvento.builder()
                .id(evento.getId())
                .status("PROCESSADO")
                .mensagem("Evento processado com sucesso")
                .valor(evento.getValor())
                .processado_em(Instant.now().toString())
                // *** ADICIONE AQUI OS CAMPOS CUSTOMIZADOS DE ResultadoEvento ***
                // .codigoAutorizacao(auth.getCodigo())
                // .clienteId(cliente.getId())
                .build();

        // Persiste o resultado para polling do frontend
        resultadoStore.salvar(resultado);
        log.info("Resultado salvo para polling: id={}, status={}", resultado.getId(), resultado.getStatus());

        // Encaminha para o próximo serviço (se kafka.destino.url estiver configurado)
        if (destinoUrl != null && !destinoUrl.isBlank()) {
            enviarParaServico(resultado);
        }

        return resultado;
    }

    // ==============================================================
    // Envia o resultado via HTTP POST para o próximo serviço da cadeia.
    //
    // ALTERNATIVAS DE DESTINO (substitua ou combine conforme necessário):
    // ==============================================================
    // OPÇÃO 1 — Redis Pub/Sub (push para serviços inscritos):
    //   redisTemplate.convertAndSend("canal-resultados", resultado);
    //
    // OPÇÃO 2 — Banco de dados (persistência duradoura):
    //   resultadoRepository.save(new ResultadoEntity(resultado));
    //
    // OPÇÃO 3 — Outro tópico Kafka (encadeamento de pipelines):
    //   kafkaTemplate.send("topico-resultados", resultado.getId(), resultado);
    //
    // OPÇÃO 4 — WebSocket (push direto para o browser):
    //   messagingTemplate.convertAndSend("/topic/resultado/" + resultado.getId(), resultado);
    // ==============================================================
    private void enviarParaServico(ResultadoEvento resultado) {
        try {
            log.info("Encaminhando resultado para: {}", destinoUrl);
            restTemplate.postForEntity(destinoUrl, resultado, Void.class);
            log.info("Resultado encaminhado com sucesso");
        } catch (Exception e) {
            // Loga o erro mas NÃO relança — o encaminhamento falhar não deve
            // impedir o commit do offset Kafka nem causar reprocessamento do evento
            log.error("Falha ao encaminhar resultado para {}: {}", destinoUrl, e.getMessage(), e);
        }
    }
}


// ==============================================================
// SERVICE: EventoConsumer
// Listener Kafka com commit manual de offset.
//
// Estratégia de tratamento de erros:
//   - JSON inválido       → acknowledge imediato (sem retry, sem DLT)
//   - Erro inesperado     → NÃO acknowledge → DefaultErrorHandler faz retry
//                         → após esgotar tentativas → mensagem vai para a DLT
// ==============================================================
@Service
@RequiredArgsConstructor
@Slf4j
class EventoConsumer {

    private final EventoConsumerService consumerService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics         = "${kafka.topico}",
            groupId        = "${kafka.grupo}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumir(ConsumerRecord<String, String> record, Acknowledgment ack) {

        log.info("Mensagem recebida — topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());

        try {
            // Desserializa o payload JSON para o model Evento
            Evento evento = objectMapper.readValue(record.value(), Evento.class);

            // Executa a lógica de negócio (ver EventoConsumerService.processarEvento)
            consumerService.processarEvento(evento);

            // Confirma o offset somente após processamento bem-sucedido
            ack.acknowledge();
            log.info("Offset confirmado — partition={}, offset={}", record.partition(), record.offset());

        } catch (JsonParseException | JsonMappingException e) {
            // Mensagem com JSON malformado: nunca será recuperável, não faz sentido reprocessar.
            // Confirma o offset para avançar e registra o erro para investigação.
            log.error("Mensagem com JSON inválido — descartando sem retry | offset={} | erro={}",
                    record.offset(), e.getMessage());
            ack.acknowledge();

        } catch (Exception e) {
            // Erro inesperado (ex: banco fora do ar, API externa indisponível):
            // NÃO confirma o offset — o DefaultErrorHandler vai:
            //   1. Reprocessar até kafka.dlq.tentativas vezes (FixedBackOff de 1s)
            //   2. Após esgotar, publicar na DLT (<topico>.DLT) e confirmar o offset
            log.error("Erro ao processar mensagem — partition={}, offset={} | erro={}",
                    record.partition(), record.offset(), e.getMessage(), e);
            throw new RuntimeException("Falha no processamento do evento", e);
        }
    }
}


// ==============================================================
// CONFIGURATION: KafkaConsumerConfig
// Configuração completa do consumer, error handler e DLT.
// Valores lidos exclusivamente do application.properties via @Value.
// ==============================================================
@Configuration
@EnableKafka
@Slf4j
class KafkaConsumerConfig {

    // Todos os valores vêm do application.properties — não altere aqui
    @Value("${kafka.broker}")
    private String broker;

    @Value("${kafka.grupo}")
    private String grupo;

    @Value("${kafka.dlq.tentativas}")
    private int dlqTentativas;

    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    // ----------------------------------------------------------
    // ConsumerFactory: configura como o consumer Kafka se conecta
    // ----------------------------------------------------------
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,    broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,             grupo);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,   false);     // commit manual via Acknowledgment
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,    "earliest"); // recomeça do início se não houver offset salvo
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,     10);        // processa até 10 mensagens por poll
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ----------------------------------------------------------
    // KafkaListenerContainerFactory: cria os containers dos listeners
    //   - concurrency=3  → 3 threads consumindo em paralelo
    //   - AckMode.MANUAL_IMMEDIATE → ack chamado manualmente no listener
    // ----------------------------------------------------------
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    // ----------------------------------------------------------
    // DefaultErrorHandler: retry + Dead Letter Topic (DLT)
    //
    //   Fluxo de uma mensagem com erro:
    //     tentativa 1 → aguarda 1s → tentativa 2 → ... → tentativa N → DLT
    //
    //   DLT: tópico criado automaticamente com sufixo ".DLT"
    //     Ex: meu-topico → meu-topico.DLT
    //
    //   Exceções "não reprocessáveis" vão direto para a DLT (sem retry):
    //     - JsonParseException    → payload JSON corrompido/inválido
    //     - IllegalArgumentException → dado de negócio inválido
    // ----------------------------------------------------------
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {

        // Publica na DLT mantendo a mesma partição do tópico original
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("DLT — enviando após {} tentativas | topic={} partition={} offset={}",
                            dlqTentativas, record.topic(), record.partition(), record.offset());
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                }
        );

        // FixedBackOff: intervalo fixo de 1s entre tentativas
        // dlqTentativas=3 → 2 retries (tentativa inicial + 2 retentativas = 3 ao total)
        FixedBackOff backOff = new FixedBackOff(1_000L, dlqTentativas - 1L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Exceções que NÃO devem ser reprocessadas — vão direto para a DLT
        handler.addNotRetryableExceptions(
                JsonParseException.class,
                JsonMappingException.class,
                IllegalArgumentException.class
        );

        return handler;
    }

    // ----------------------------------------------------------
    // ProducerFactory e KafkaTemplate: usados pelo DeadLetterPublishingRecoverer
    // para publicar mensagens na DLT. O consumer precisa de capacidade de produção
    // apenas para este fim.
    // ----------------------------------------------------------
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,        broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,     StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG,                     "all");  // durabilidade máxima para a DLT
        props.put(ProducerConfig.RETRIES_CONFIG,                  3);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ----------------------------------------------------------
    // RestTemplate: usado por EventoConsumerService.enviarParaServico()
    // para encaminhar resultados via HTTP POST
    // ----------------------------------------------------------
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    // ----------------------------------------------------------
    // ObjectMapper: usado por EventoConsumer para desserializar
    // o payload JSON das mensagens Kafka manualmente
    // ----------------------------------------------------------
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
