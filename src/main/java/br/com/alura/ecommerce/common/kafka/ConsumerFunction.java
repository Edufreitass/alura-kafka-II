package br.com.alura.ecommerce.common.kafka;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

// Interface que recebe um record
public interface ConsumerFunction<T> {
	// Ao chamar o método consume da Interface ConsumerFunction é executado a implementação do parse na classe KafkaService
	void consume(ConsumerRecord<String, T> record) throws ExecutionException, InterruptedException ;
}
