package com.kafka.config;

import static com.kafka.config.KafkaTopicConfig.TOPIC_NAME;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamConfig {

	private static final String APPLICATION_01 = "my-application-01";
	private static final String STORE_NAME = "my-store-01";
	@Value(value = "${kafka.bootstrapAddress}")
	private String bootstrapAddress;
	KafkaStreams streams;

	@Bean
	public void kafkaStream() {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_01);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textLines = builder.stream(TOPIC_NAME);
		KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore(STORE_NAME);
		Predicate<String, String> predicate = (k, v) -> {
			// Condition to skip invalid record
			return !k.contains(" ");
		};
		textLines.filter(predicate).toTable(Materialized.as(keyValueBytesStoreSupplier));
		streams = new KafkaStreams(builder.build(), props);
		streams.start();

	}

	/**
	 * Get Value by key
	 * 
	 * @param key
	 * @return
	 */
	public Object get(String key) {
		StoreQueryParameters<ReadOnlyKeyValueStore<Object, Object>> storeQueryParameters = StoreQueryParameters
				.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore()).withPartition(0);
		ReadOnlyKeyValueStore<Object, Object> keyValueStore = streams.store(storeQueryParameters);

		return keyValueStore.get(key);

	}

	public List<Object> range(String key, int from, int to) {
		List<Object> list = new ArrayList<>();
		StoreQueryParameters<ReadOnlyKeyValueStore<Object, Object>> storeQueryParameters = StoreQueryParameters
				.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore());
		ReadOnlyKeyValueStore<Object, Object> keyValueStore = streams.store(storeQueryParameters);
		Iterator<?> iterator = keyValueStore.range(key + from, key + to);
		while (iterator.hasNext()) {
			list.add((Object) iterator.next());
		}
		return list;

	}
	
	/**Get value Range
	 * @param key
	 * @param from
	 * @param to
	 * @return
	 */
	public List<Object> rangeString(String key, String from, String to) {
		List<Object> list = new ArrayList<>();
		StoreQueryParameters<ReadOnlyKeyValueStore<Object, Object>> storeQueryParameters = StoreQueryParameters
				.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore());
		ReadOnlyKeyValueStore<Object, Object> keyValueStore = streams.store(storeQueryParameters);
		Iterator<?> iterator = keyValueStore.range(key + from, key + to);
		while (iterator.hasNext()) {
			list.add((Object) iterator.next());
		}
		return list;

	}
}
