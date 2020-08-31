package br.com.alura.ecommerce.common.kafka;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// KafkaDispacher será o PRODUCER
public class KafkaDispatcher<T> implements Closeable {

	// Variavel do produtor
	private final KafkaProducer<String, T> producer;

	// Construtor
	public KafkaDispatcher() {
		this.producer = new KafkaProducer<>(properties());
	}

	// Método que contém as configurações das propriedades do KafkaProducer
	private static Properties properties() {
		// Criação das propriedades
		var properties = new Properties();
		// Diz ao Kafka onde ele irá se conectar(rodar).
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		// Diz ao Kafka que, tanto a chave quanto o valor, os dois irão transformar a
		// mensagem e a chave baseado em Strings,
		// é necessario passar tranformadores de Strings para Bytes, ou seja,
		// Serializadores de Strings para Bytes.
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// Diz ao Kafka que, tanto a chave quanto o valor, os dois irão transformar a
		// mensagem e a chave baseado em Strings,
		// é necessario passar tranformadores de Strings para Bytes, ou seja,
		// Serializadores de Strings para Bytes.
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
		// Retorna as propriedades
		return properties;
	}

	// Se <T> é String, OK. Se <T> é Order, OK. Ele envia o valor de qualquer forma, pois ele é genérico.
	public void send(String topic, String key, T value) throws InterruptedException, ExecutionException {
		// Registro do Producer que recebe como paramêtros(Topic, Key, Value)
		var record = new ProducerRecord<>(topic, key, value);
		// Envia uma mensagem, que neste caso será um record(um registro), pois ficará registrado no kafka, por quanto tempo?
		// Depende das configurações do serverProperties. O método "send" não é blocante, ele não segura, é assíncrono!
		// Ele necessita do método get(), para esperar o Future(método que o send
		// devolve) terminar e também de algumas exceções que serão geradas automaticamente.
		// Método send() tem uma variação que recebe um "Callback", data = dados de sucesso, ex = exceptions de falha
		Callback callback = (data, ex) -> {
			// Se a exception foi diferente de nulo, é porque deu erro, imprime e encerra
			if (ex != null) {
				ex.printStackTrace();
				return;
			}
			// Se a exception foi nulo, é porque deu sucesso
			// Imprime (Topico, Partição, Offset(posição) e o Timestamp(em que momento)
			System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset "
					+ data.offset() + "/ timestamp " + data.timestamp());
		};
		// Listener extraido para imprimir as mesmas mensagens de sucesso ou erro!
		producer.send(record, callback).get();
	}

	@Override
	public void close() {
		producer.close();
	}

}
