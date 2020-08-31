package br.com.alura.ecommerce.service.neworder;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import br.com.alura.ecommerce.common.kafka.KafkaDispatcher;
import br.com.alura.ecommerce.service.fraud.detector.Order;

// PRODUCER
// Classe responsável por produzir uma mensagem que será enviada ao tópico ECOMMERCE_NEW_ORDER(NOVO_PEDIDO).
public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// Produz uma mensagem no kafka.
		try (var orderDispatcher = new KafkaDispatcher<Order>()){ // despacha uma Order
			try (var emailDispatcher = new KafkaDispatcher<String>()){ // despacha uma String
				// Laço para executar 100 mensagens
				for (var i = 0; i < 10; i++) {
	
					// Variável que recebe uma "Universally Unique Identifier (UUID)" ou ID UNIVERSAL
					// Esse ID será gerado de forma aleatória e será transformado para uma String
					var userId = UUID.randomUUID().toString();
					var orderId = UUID.randomUUID().toString();
					var amount = new BigDecimal(Math.random() * 5000 + 1);
					
					var order = new Order(userId, orderId, amount);
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
	
					// Váriavel que recebe a mensagem do EMAIL
					var email = "Thank you for your order! We are processing your order!";
					// Envia um email
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
				}
			}
		}
	}
}
