package br.com.alura.ecommerce.service.log;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.alura.ecommerce.common.kafka.KafkaService;

// CONSUMER
// Classe responsável por detectar fraudes no sistema.
public class LogService {

	public static void main(String[] args) {
		
		var logService = new LogService();
		try (var service = new KafkaService(LogService.class.getSimpleName(),
				// Pattern é um tipo de Regex ou Expressão Regular
				Pattern.compile("ECOMMERCE.*"),
				// Method References(função) que recebe o ConsumerRecord
				logService::parse,
				String.class,
				Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))){ 
			service.run();
		}
	}
	
	private void parse(ConsumerRecord<String, String> record) {
			System.out.println("-------------------------------------------");
			// Imprime o TÓPICO
			System.out.println("LOG: " + record.topic());
			// Imprime a CHAVE
			System.out.println(record.key());
			// Imprime o VALOR da mensagem
			System.out.println(record.value());
			// Imprime a PARTIÇÃO onde foi enviada
			System.out.println(record.partition());
			// Imprime a POSIÇÃO da mensagem
			System.out.println(record.offset());
	}
}
