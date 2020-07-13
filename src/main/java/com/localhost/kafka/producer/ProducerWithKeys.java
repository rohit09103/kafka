/**
 *
 */
package com.localhost.kafka.producer;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author rohit
 * @date 13-Jul-2020
 */
public class ProducerWithKeys {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProducerWithKeys.class);

	private static final String BOOTSTRAP_SERVER = "bootstrap.server";
	private static final String BOOTSTRAP_SERVER_IP = "localhost:9092";
	private static final String KEY_SERIALIZER = "key.serializer";
	private static final String VALUE_SERIALIZER = "value.serializer";
	private static final String TOPIC_NAME = "my_first_topic";

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void main(final String[] args) throws InterruptedException, ExecutionException {
		// producer properties.
		final Properties properties = new Properties();
		properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_IP);
		properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// creating producer.
		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {
			// send data
			final String value = "Hello World! " + Integer.toString(i);
			final String key = "id_" + Integer.toString(i);
			final ProducerRecord<String, String> records = new ProducerRecord<String, String>(TOPIC_NAME, key, value);
//			Thread.sleep(1000);
			LOGGER.info("Key: {}.", key);
			// this is in async call. does not send data instantly after executing send,
			// need to flush it.
			producer.send(records, new Callback() {

				public void onCompletion(final RecordMetadata metadata, final Exception exception) {
					if (null == exception) {
						LOGGER.info("Record metadata recieved.");
						LOGGER.info("Topic: {}.", metadata.topic());
						LOGGER.info("Topic: {}.", metadata.topic());
						LOGGER.info("Partition: {}.", metadata.partition());
						LOGGER.info("Offset: {}.", metadata.offset());
						LOGGER.info("TimeStamp: {}.", metadata.timestamp());
					} else {
						LOGGER.error("Exception has occured while sending data to topic with message: {}.",
								exception.getMessage());
					}
				}
			}).get(); // blocking the send to make in sync.

		}
		// this will make the producer send data to kafka topic.
		producer.flush();

		// flush and close prodcuer.
		producer.close();
	}

}
