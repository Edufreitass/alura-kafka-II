package br.com.alura.ecommerce.common.kafka;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaService<T> implements Closeable {

	private final KafkaConsumer<String, T> consumer;
	private final ConsumerFunction parse;

	// Construtor que recebe o topico e uma função de
	// parse(função que recebe o record e não devolve nada)
	public KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
		this(parse, groupId, type, properties);
		// consumer que irá consumir as mensagens do tópico ECOMMERCE_SEND_EMAIL
		consumer.subscribe(Collections.singletonList(topic));

	}

	// Construtor que recebe o topico como PATTERN e uma função de
	// parse(função que recebe o record e não devolve nada)
	public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
		this(parse, groupId, type, properties);
		// consumer que irá consumir as mensagens do tópico ECOMMERCE_SEND_EMAIL
		consumer.subscribe(topic);

	}

	private KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));
	}

	// Método que vai rodar o KafkaService
	public void run() {
		// Laço para deixar o serviço rodando, escutando as mensagens!
		while (true) {
			// consumer que irá perguntar se tem alguma mensagem no tópico, por algum tempo
			// ele irá devolver vários registros, os registros que foram enviados!
			var records = consumer.poll(Duration.ofMillis(100));
			// Se ele encontrar, ele imprime quantos ele encontrou
			if (!records.isEmpty()) {
				System.out.println("Encontrei " + records.count() + " registros");
				// Se encontrou registros será feito um loop, no qual irá imprimir,
				for (var record : records) {
					// Para cada record existente, será chamado o "parse"
					try {
						parse.consume(record);
					} catch (Exception e) {
						// only catches Exception because no matter which Exception
						// i want to recover and parse the next one
						// so far, just logging the exception for this message
						e.printStackTrace();
					} 
				}
			}

		}
	}

	// Método que contém as configurações das propriedades do KafkaConsumer
	private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
		// Criação das propriedades
		var properties = new Properties();
		// Diz ao Kafka aonde ele irá se conectar(rodar).
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		// Configuração do Deserializador da CHAVE, no qual antes era Serializada de String para Bytes,
		// E agora será Deserializada de Bytes para String
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// Configuração do Deserializador da VALOR, no qual antes era Serializada de String para Bytes,
		// E agora será Deserializada de Bytes para String
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		// Configuração do Grupo do consumer, para que ele(EmailService) possa receber todas a mensagens
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		// Configuração do Client, que irá gerar um random UUID
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		// Configuração customizada do Generic Type
		properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
		// Tudo o que for sobreescrito, esse overrideProperties sobreescreve!
		properties.putAll(overrideProperties);
		// Retorno das propriedades
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}

}
