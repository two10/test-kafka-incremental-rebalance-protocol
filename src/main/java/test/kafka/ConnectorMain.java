package test.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorMain {

	
	
	public static final long DELAY_IN_PARTITION_REVOKE = 5000;
	public static String TOPIC = "quickstart-events";
	public static Long MAX_DELAY_IN_ASSIGNMENT = 1000l;
	public static Integer INITIAL_NUMBER_OF_CONNECTOR = 0;
	
	public static AtomicInteger connectorNum = new AtomicInteger(0);
	
	public static void main(String[] args) {
		
		Integer initialNumberOfConnector= INITIAL_NUMBER_OF_CONNECTOR;
		
		
		
		for(int i=0;i<initialNumberOfConnector;i++) {
			
			startConnector();
			
		}
		
		
		while(true) {
			
			 // Enter data using BufferReader
	        BufferedReader reader = new BufferedReader(
	            new InputStreamReader(System.in));
	 
	        
	        System.out.println("PRESS ENTER TO START A NEW CONNECTOR");
	        // Reading data using readLine
	        String name;
			try {
				name = reader.readLine();
				
				startConnector();
				
			} catch (IOException e) {
			}
	 
	        
		}
		
		
		
	}

	private static void startConnector() {
		Thread thread = new Thread(new Runnable() {
			
			public void run() {
				// TODO Auto-generated method stub
				TestKafkaClient testKafkaClient = new TestKafkaClient(connectorNum.getAndIncrement());
				testKafkaClient.startAConnector();
			}
		});
		thread.start();
	}
	
}
