/**
 *
 */
package com.localhost.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.localhost.kafka.common.CommonConstant;

/**
 * @author rohit
 * @date 13-Jul-2020
 */
public class Consumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

	/**
	 * @param args
	 */
	public static void main(final String[] args) {

		final Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.BOOTSTRAP_SERVER_IP);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CommonConstant.GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

		kafkaConsumer.subscribe(Collections.singleton(CommonConstant.TOPIC_NAME));

		while (true) {
			final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
			for (final ConsumerRecord<String, String> record : records) {
				LOGGER.info("Key: {}, Value: {}.", record.key(), record.value());
				LOGGER.info("Partition: {}, Offset: {}.", record.partition(), record.offset());
			}
		}
	}

}
