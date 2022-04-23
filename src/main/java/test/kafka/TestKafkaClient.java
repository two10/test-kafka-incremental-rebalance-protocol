package test.kafka;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestKafkaClient {

	private  String clientStartTime = "";
	private  String clientSubscriptionAskedTime = "";
	private  String partitionAssignedTime = "";
	private  String partitionRevokeTime = "";
	private  String partitionLostTime = "";

	private  long clientStartTimeMillis = System.currentTimeMillis();
	private  long clientSubscriptionAskedTimeMillis = System.currentTimeMillis();
	private  long partitionAssignedTimeMillis = System.currentTimeMillis();
	private  long partitionRevokeTimeMillis = 0;
	private  long partitionLostTimeMillis = System.currentTimeMillis();

	
	  private static final Logger logger = LogManager.getLogger(TestKafkaClient.class);
	private Integer connectorNumber ; 
	
	public TestKafkaClient(Integer connectorNumber) {
		this.connectorNumber = connectorNumber;


	}
	
	public  class LogTime implements ConsumerRebalanceListener {
		private Consumer<?, ?> consumer;

		public LogTime() {

		}

		public LogTime(Consumer<?, ?> consumer) {
			this.consumer = consumer;
		}

		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			// save the offsets in an external store using some custom code not described
			// here
			partitionRevokeTime = getTime();
			partitionRevokeTimeMillis = System.currentTimeMillis();
			logger.info(connectorNumber+" partitionRevokeTime " + partitionRevokeTime);
			logger.info(connectorNumber+" partition revoked ");
			for (TopicPartition partition : partitions) {
				logger.info(connectorNumber+" partition revoked  "+partition.toString() + " ");
				RevokeAssignMapper.partitionRevoked(connectorNumber, partition.toString(),partitionRevokeTimeMillis );
			}
			logger.info("");
			
			
			try {
				Thread.sleep(ConnectorMain.DELAY_IN_PARTITION_REVOKE);
			} catch (InterruptedException e) {
			}
		}

		public void onPartitionsLost(Collection<TopicPartition> partitions) {
			partitionLostTime = getTime();
			logger.info("partitionLostTime  " + partitionLostTime);
			// do not need to save the offsets since these partitions are probably owned by
			// other consumers already
		}

		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			// read the offsets from an external store using some custom code not described
			// here
			partitionAssignedTime = getTime();
			partitionAssignedTimeMillis = System.currentTimeMillis();
		

			logger.info(connectorNumber+" partitionAssignedTime " + partitionAssignedTime);
			logger.info(connectorNumber+" "+"partition assigned ");
			for (TopicPartition partition : partitions) {
				logger.info(connectorNumber+" partition assigned "+partition.toString() + " ");
				RevokeAssignMapper.partitionAssigned(connectorNumber, partition.toString(),partitionAssignedTimeMillis );
			}
			logger.info("");
			
			
		}

		private String getTime() {
			DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
			LocalDateTime localDateTime = LocalDateTime.now();

			String format = fmt.format(localDateTime);
			return format;
		}

	}

	private static String getTime() {
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
		LocalDateTime localDateTime = LocalDateTime.now();

		String format = fmt.format(localDateTime);
		return format;
	}

	public  void startAConnector() {

		clientStartTime = getTime();
		clientStartTimeMillis = System.currentTimeMillis();
		logger.info("clienStartTime " + clientStartTime);

		String topic = ConnectorMain.TOPIC;
		String group = "g1";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", group);
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");

		props.put("session.timeout.ms", "12000");
		props.put("heartbeat.interval.ms", "1000");

		props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
				test.kafka.MyAssignor.class.getName());
		props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		consumer.subscribe(Arrays.asList(topic), new LogTime());
		clientSubscriptionAskedTimeMillis = System.currentTimeMillis();
		clientSubscriptionAskedTime = getTime();
		logger.info("subscription time " + clientSubscriptionAskedTime);
		logger.info("Subscribed to topic " + topic);
		int i = 0;

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			Long startTime = System.currentTimeMillis();
			consumer.commitSync();
			Long endTime = System.currentTimeMillis();			
			logger.info("commit time "+ (endTime-startTime));
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
		}
	}

}
