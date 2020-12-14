package com.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.config.KafkaStreamConfig;
import com.kafka.config.KafkaTopicConfig;

@SpringBootApplication
@RestController
public class KafkaApplication {

	@Autowired
	KafkaStreamConfig kafkaStreamConfig;

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	@GetMapping("/postmsg/{value}")
	public String postMessage(@PathVariable("value") String value) {
		sendMessage(value);

		return "";
	}

	@GetMapping("/readData/{key}")
	public Object readData(@PathVariable("key") String key) {

		return kafkaStreamConfig.get(key);
	}

	@GetMapping("/rangeString/{key}/{from}/{to}")
	public Object rangeString(@PathVariable("key") String key, @PathVariable("from") String from,
			@PathVariable("to") String to) {

		return kafkaStreamConfig.rangeString(key, from, to);
	}

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String message) {
		String[] splittedMsg = message.split("=");
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(KafkaTopicConfig.TOPIC_NAME,
				splittedMsg[0], splittedMsg[1]);
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(producerRecord);

		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				System.out.println(
						"Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
			}
		});
	}
}
