package com.demo;

import com.demo.engine.Producer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class SpringBootWithKafkaApplicationTests extends AbstractIntegrationTest {

	@Autowired
	private TestRestTemplate restTemplate;

	@Autowired
	private ConfigurableApplicationContext ctx;

	@Test
	public void assertMessageInTheTopic() {
		// given
		MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
		final String message = "This is a message";
		map.add("message", message);
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
		HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(map, headers);

		// when
		final ResponseEntity<String> responseEntity = restTemplate.postForEntity("/kafka/publish", request, String.class);

		// then
		Assert.assertEquals(200, responseEntity.getStatusCodeValue());
		Properties kafkaProps = new Properties();
		final String bootstrapServers = ctx.getEnvironment().getProperty("spring.kafka.consumer.bootstrap-servers");
		final String keyDeserializer = ctx.getEnvironment().getProperty("spring.kafka.consumer.key-deserializer");
		final String valueDeserializer = ctx.getEnvironment().getProperty("spring.kafka.consumer.value-deserializer");
		kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
		kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
		kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		try (Consumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
			consumer.subscribe(Arrays.asList(Producer.TOPIC));
			assertEventually(5000, () -> {
				final ConsumerRecords<String, String> records = consumer.poll(100);
				return 1 == records.count() && message.equals(records.records(Producer.TOPIC).iterator().next().value());
			});
		}

	}

	private void assertEventually(long timeout, Supplier<Boolean> supplier) {
		final long start = System.currentTimeMillis();
		boolean isSuccess;
		do {
			isSuccess = supplier.get();
		} while (!isSuccess && System.currentTimeMillis() - start < timeout);
		Assert.assertTrue(isSuccess);
	}

}
