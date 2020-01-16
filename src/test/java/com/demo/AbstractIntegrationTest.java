package com.demo;

import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.testcontainers.containers.KafkaContainer;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = SpringBootWithKafkaApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = AbstractIntegrationTest.Initializer.class)
public abstract class AbstractIntegrationTest {

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            final String bootstrapServers = kafka.getBootstrapServers().replace("PLAINTEXT://", "");
            TestPropertyValues values = TestPropertyValues.of(
                    "spring.kafka.producer.bootstrap-servers=" + bootstrapServers,
                    "spring.kafka.consumer.bootstrap-servers=" + bootstrapServers
            );
            values.applyTo(configurableApplicationContext);
        }
    }

}
