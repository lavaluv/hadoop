import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class KafkaWordCount {
	public static void main(String[] args)throws Exception{
		//streams excution configuration map to StreamsConfig
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams_wordCount");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		//the serialization and deserialization type
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		StreamsBuilder builder = new StreamsBuilder();
		//input source from topic
		KStream<String, String> source = builder.stream("input");
		KTable<String, Long> counts = source
				// Split each text line, by whitespace, into words.
				.flatMapValues( value -> 
				Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
				// Group the text words as message keys,ignore key
				.groupBy((String key,String value) -> value)
				// Count the occurrences of each word (message key).
				.count();
		// Store the running counts as a changelog stream to the output topic.
		counts.toStream().to("output",Produced.with(Serdes.String(), Serdes.Long()));
		final KafkaStreams streams = new KafkaStreams(builder.build(), properties);
		final CountDownLatch latch = new CountDownLatch(1);
		
		Runtime.getRuntime().addShutdownHook(new Thread("wordCount-shutDown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});
		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			// : handle exception
			System.exit(1);
		}
		System.exit(0);
	}
}
