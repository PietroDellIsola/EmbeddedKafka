package com.example.kafka;

import com.example.kafka.dto.MessageDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
class KafkaApplicationTests {
	private static final String CONSUMER_APP_ID = "consumer_id";
	private static final String CONSUMER_GROUP_ID = "group_id";
	private static final String TOPIC1 = "calcio";
	private static final String TOPIC2 = "cucina";
	@Test
	void kafkaTest() {
		MessageDto msg1 = MessageDto.builder().message(
				"Ancora porta inviolata per G.Buffon! In questi mesi è previsto l'incontro con la dirigenza per il rinnovo del contratto col Parma")
				.version("1.0").build();
		MessageDto msg2 = MessageDto.builder().message("Ibrahimović:\"Nel Milan fino a fine carriera!\"").version("1.0").build();
		MessageDto msg3 = MessageDto.builder().message("Prova i prodotti Garden Gourmet! Per te uno sconto del 20%!!").version("1.0").build();
		KafkaProducer<String, MessageDto> producer = createKafkaProducer();
		producer.send(new ProducerRecord<String, MessageDto>(TOPIC1, "1", msg1));
		System.out.println("msg1 inviato: " + msg1);
		producer.send(new ProducerRecord<String, MessageDto>(TOPIC1, "2", msg2));
		System.out.println("msg2 inviato: " + msg2);
		producer.send(new ProducerRecord<String, MessageDto>(TOPIC2, "3", msg3));
		System.out.println("msg3 inviato: " + msg3);
		producer.close();
		AtomicReference<MessageDto> msgCons = new AtomicReference<>();

		//consumer 1
		KafkaConsumer<String, MessageDto> consumer = createKafkaConsumer();
		ArrayList<String> topicsList = new ArrayList<>();
		topicsList.add(TOPIC1);
		//consumer.subscribe(Arrays.asList(TOPIC1));
		consumer.subscribe(topicsList);
		ConsumerRecords<String, MessageDto> records = consumer.poll(Duration.ofSeconds(3));
		records.forEach(record -> {
			msgCons.set(record.value());
			System.out.println("[Consumer 1] Messaggio ricevuto: " + record.value());
		});
		consumer.close();

		//consumer 2
		consumer = createKafkaConsumer();
		topicsList = new ArrayList<>();
		topicsList.add(TOPIC2);
		consumer.subscribe(topicsList);
		records = consumer.poll(Duration.ofSeconds(3));
		records.forEach(record -> {
			msgCons.set(record.value());
			System.out.println("[Consumer 2] Messaggio ricevuto: " + record.value());
		});
		consumer.close();
	}

	private static KafkaProducer<String, MessageDto> createKafkaProducer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CONSUMER_APP_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.example.kafka.mapper.CustomSerializer");
		return new KafkaProducer<>(props);
	}

	private static KafkaConsumer<String, MessageDto> createKafkaConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_APP_ID);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.example.kafka.mapper.CustomDeserializer");
		return new KafkaConsumer<>(props);
	}

}
