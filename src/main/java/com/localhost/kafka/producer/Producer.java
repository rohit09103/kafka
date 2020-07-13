/**
 *
 */
package com.localhost.kafka.producer;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author rohit
 * @date 13-Jul-2020
 */
public class Producer {

	private static final String BOOTSTRAP_SERVER = "bootstrap.server";
	private static final String BOOTSTRAP_SERVER_IP = "localhost:9092";
	private static final String KEY_SERIALIZER = "key.serializer";
	private static final String VALUE_SERIALIZER = "value.serializer";
	private static final String TOPIC_NAME = "my_first_topic";

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		// producer properties.
		final Properties properties = new Properties();
		properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_IP);
		properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// creating producer.
		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// send data
		final ProducerRecord<String, String> records = new ProducerRecord<String, String>(TOPIC_NAME, "Hello World!");
		// this is in async call. does not send data instantly after executing send,
		// need to flush it.
		producer.send(records);
		// this will make the producer send data to kafka topic.
		producer.flush();

		// flush and close prodcuer.
		producer.close();
	}

}
