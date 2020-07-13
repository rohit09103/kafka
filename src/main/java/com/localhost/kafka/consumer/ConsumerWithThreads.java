/**
 *
 */
package com.localhost.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.localhost.kafka.common.CommonConstant;

/**
 * @author rohit
 * @date 13-Jul-2020
 */
public class ConsumerWithThreads {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWithThreads.class);

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws InterruptedException {

		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final ConsumerThread consumerThread = new ConsumerThread(countDownLatch, "con-4", CommonConstant.TOPIC_NAME);
		final Thread consumer = new Thread(consumerThread);
		consumer.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOGGER.info("Caught shutdown hook.");
			consumerThread.shutdown();
			try {
				countDownLatch.await();
			} catch (final InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}));

		countDownLatch.await();

	}

	private static class ConsumerThread implements Runnable {

		private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerThread.class);

		final KafkaConsumer<String, String> kafkaConsumer;
		private final CountDownLatch countDownLatch;

		public ConsumerThread(final CountDownLatch countDownLatch, final String groupId, final String topic) {
			this.countDownLatch = countDownLatch;
			final Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.BOOTSTRAP_SERVER_IP);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			kafkaConsumer = new KafkaConsumer<>(properties);
			kafkaConsumer.subscribe(Collections.singleton(topic));
		}

		@Override
		public void run() {
			try {
				while (true) {
					final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
					for (final ConsumerRecord<String, String> record : records) {
						LOGGER.info("Key: {}, Value: {}.", record.key(), record.value());
						LOGGER.info("Partition: {}, Offset: {}.", record.partition(), record.offset());
					}
				}
			} catch (final WakeupException wakeupException) {
				LOGGER.info("Recieved shutdown signal.");
			} finally {
				kafkaConsumer.close();
				countDownLatch.countDown();
			}

		}

		public void shutdown() {
			kafkaConsumer.wakeup();
		}
	}

}
