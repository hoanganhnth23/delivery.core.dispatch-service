package com.delivery.saga_orchestrator_service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
		"spring.datasource.url=jdbc:h2:mem:saga;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
		"spring.datasource.driver-class-name=org.h2.Driver",
		"spring.jpa.hibernate.ddl-auto=create-drop",
		"spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
		"spring.kafka.listener.auto-startup=false",
		"spring.flyway.enabled=false",
		"app.outbox.relay-enabled=false"
})
class SagaOrchestratorServiceApplicationTests {

	@Test
	void contextLoads() {
	}

}
