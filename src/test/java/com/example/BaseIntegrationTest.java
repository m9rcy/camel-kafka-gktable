package com.example;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = {
        MySpringBootApplication.class,  // Your main application class
    IntegrationTestConfiguration.class
})
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=localhost:9092",
    "spring.kafka.streams.bootstrap-servers=localhost:9092",
    "spring.kafka.streams.application-id=test-app",
    "kafka.topic.order-window-topic=test-order-window",
    "kafka.topic.order-window-filtered-topic=test-order-window-filtered",
    "camel.springboot.main-run-controller=true"
})
public abstract class BaseIntegrationTest {
    // Common test setup can go here
}