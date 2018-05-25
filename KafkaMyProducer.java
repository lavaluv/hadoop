import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.errors.AuthenticationException;
//import org.apache.kafka.common.errors.OutOfOrderSequenceException;
//import org.apache.kafka.common.errors.ProducerFencedException;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import kafka.common.KafkaException;

public class KafkaMyProducer {
	public static void main(String[] args)throws Exception{
		//No transaction
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.mx", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
		//produce data
		for(int i = 0; i < 1; i++) {
			producer.send(new ProducerRecord<String, String>(
					"topicA", Integer.toString(i),Integer.toString(i)));
		}
		producer.close();
		//Use transaction
		//Must set topic config: replication.factor > 2, min.insync.replicas = 2
//		Properties properties = new Properties();
//		properties.put("bootstrap.servers", "localhost:9092");
//		properties.put("transactional.id", "my_transaction");
//		KafkaProducer<String, String> producer = new KafkaProducer<>(
//				properties,new StringSerializer(),new StringSerializer());
//		producer.initTransactions();
//		try {
//			producer.beginTransaction();
//			for(int i = 0; i< 100; i++) {
//				producer.send(new ProducerRecord<String, String>(
//						"topicA", Integer.toString(i),Integer.toString(i)));
//				producer.commitTransaction();
//			}
//		}catch (ProducerFencedException | OutOfOrderSequenceException | AuthenticationException e) {
//			//Can't recover
//			producer.close();
//		}catch (KafkaException e) {
//			//abort and retry
//			producer.abortTransaction();
//		}
//		producer.close();
	}
}
