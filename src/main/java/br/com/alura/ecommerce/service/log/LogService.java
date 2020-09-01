package br.com.alura.ecommerce.service.log;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.alura.ecommerce.common.kafka.KafkaService;

public class LogService {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		
		var logService = new LogService();
		try (var service = new KafkaService(LogService.class.getSimpleName(),
				Pattern.compile("ECOMMERCE.*"),
				logService::parse,
				String.class,
				Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))){ 
			service.run();
		}
	}
	
	private void parse(ConsumerRecord<String, String> record) {
			System.out.println("-------------------------------------------");
			System.out.println("LOG: " + record.topic());
			System.out.println(record.key());
			System.out.println(record.value());
			System.out.println(record.partition());
			System.out.println(record.offset());
	}
}
