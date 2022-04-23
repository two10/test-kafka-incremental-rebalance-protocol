#Purpose 
This poc was created for kafka client to see how incremental repartition also works  
and how much time it take to reassign partitions after they are revoked 

Working was understood from the link - https://www.youtube.com/watch?v=kJEtxWGpv3Q
  
#How to Run 
1. start a local kafka cluster
2. create a topic with 100 partition on that 
3. In ConnectorMain class on static variable provide topic name 
4. Now run the ConnectMain class 
	- this class will ask you to press enter to start a consumer 
	- you can press as many enter , that many consumers will be created 
	- Now to test a scenario on how much time a rebalance take when a new consumer is added
	- To get the time taken run command ``grep 'with delay of' logs/propertieslogs.log `` on the command line
	- this will give you delay in milliseconds   
	
	
	
#How it works , if you want to change

- it creates a TestKafkClient every time a user press enter , creation of kafka consumer is done by ConnectorMain class 
- Whenever a rebalance happen TestKafkaClient onPartition revoked and assigned  would call a bookkeeper RevokeAssignMapper
- RevokeAssignMapper checks on the time of partition from revoked to assign , if that time greater than a threshold <configurable from ConnectorMain> , it will log error 
