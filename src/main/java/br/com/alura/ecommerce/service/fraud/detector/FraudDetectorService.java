package br.com.alura.ecommerce.service.fraud.detector;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.common.kafka.KafkaDispatcher;
import br.com.alura.ecommerce.common.kafka.KafkaService;

// CONSUMER
// Classe responsável por detectar fraudes no sistema.
public class FraudDetectorService {

	public static void main(String[] args) {

		var fraudService = new FraudDetectorService();
		try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), 
				"ECOMMERCE_NEW_ORDER",
				fraudService::parse,
				Order.class,
				Map.of())){ // new HashMap<>() ou new HashMap<String, String>()
			service.run();
		}
		
	}
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	private void parse(ConsumerRecord<String, Order> record) throws InterruptedException, ExecutionException {
		System.out.println("-------------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		// Imprime a CHAVE
		System.out.println(record.key());
		// Imprime o VALOR da mensagem
		System.out.println(record.value());
		// Imprime a PARTIÇÃO onde foi enviada
		System.out.println(record.partition());
		// Imprime a POSIÇÃO da mensagem
		System.out.println(record.offset());
		// try catch para simular um processamento de fraude, terá um tempo de sleep
		// entre um record e outro de 5000 millis
		try {
			Thread.sleep(5000); // millis:5000 = 5 segundos
		} catch (InterruptedException e) {
			// ignoring
			e.printStackTrace();
		}
		// Retorna uma order
		var order = record.value();
		if (isFraud(order)) {
			// pretending that the fraud happens when the amount is >= 4500
			System.out.println("Order is a fraud!!! (" + order + ")");
			// Envia mensagens ao tópico de pedidos rejeitados
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
		} else {
			System.out.println("Approved: " + order);
			// Envia mensagens ao tópico de pedidos aprovados
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getUserId(), order);
		}
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}
	
}
