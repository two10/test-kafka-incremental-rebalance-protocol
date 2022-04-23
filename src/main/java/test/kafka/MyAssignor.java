package test.kafka;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;

public  class MyAssignor extends CooperativeStickyAssignor {
	@Override
	public List<RebalanceProtocol> supportedProtocols() {
		return Arrays.asList(RebalanceProtocol.COOPERATIVE);
	}

}