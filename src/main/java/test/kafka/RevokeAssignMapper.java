package test.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RevokeAssignMapper {

	private static final Logger logger = LogManager.getLogger(TestKafkaClient.class);

	private static Map<String, Long> partitionVsRevokeTime = new ConcurrentHashMap<String, Long>();
	private static Map<String, Long> partitionVsAssignTime = new ConcurrentHashMap<String, Long>();
	private static Map<String, Integer> partitionVsConsumerid = new ConcurrentHashMap<String, Integer>();

	public static void partitionRevoked(Integer consumerId, String partition, Long timeInMillis) {

		synchronized (partition) {
			partitionVsRevokeTime.put(partition, timeInMillis);
		}

	}

	public static void partitionAssigned(Integer consumerId, String partition, Long timeInMillis) {

		
		
		synchronized (partition) {
			Integer consumerIdAss = partitionVsConsumerid.get(partition);
			Long assignTime = partitionVsAssignTime.get(partition);
			Long revokeTime = partitionVsRevokeTime.get(partition);

			
			partitionVsAssignTime.put(partition, timeInMillis);
			partitionVsConsumerid.put(partition, consumerId);
			
			if (consumerIdAss != null && revokeTime !=null && !consumerId.equals(consumerIdAss)) {

				Long timeTakeToReassign = (timeInMillis - revokeTime);
				if (timeTakeToReassign > ConnectorMain.MAX_DELAY_IN_ASSIGNMENT) {
					logger.error("partition " + partition + " assigned from consumer " + consumerIdAss + " to "
							+ consumerId + " with delay of" + timeTakeToReassign);
				}

			}
			
			
			

		}

	}

}
