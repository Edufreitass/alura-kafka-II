package br.com.alura.ecommerce.service.fraud.detector;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.common.kafka.KafkaDispatcher;
import br.com.alura.ecommerce.common.kafka.KafkaService;

public class FraudDetectorService {

	public static void main(String[] args) {

		var fraudService = new FraudDetectorService();
		try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), 
				"ECOMMERCE_NEW_ORDER",
				fraudService::parse,
				Order.class,
				Map.of())){ 
			service.run();
		}
		
	}
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	private void parse(ConsumerRecord<String, Order> record) throws InterruptedException, ExecutionException {
		System.out.println("-------------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(5000); // millis:5000 = 5 segundos
		} catch (InterruptedException e) {
			// ignoring
			e.printStackTrace();
		}
		var order = record.value();
		if (isFraud(order)) {
			// pretending that the fraud happens when the amount is >= 4500
			System.out.println("Order is a fraud!!! (" + order + ")");
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
		} else {
			System.out.println("Approved: " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
		}
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}
	
}
