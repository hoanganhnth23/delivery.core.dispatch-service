package com.delivery.saga_orchestrator_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SagaOrchestratorServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(SagaOrchestratorServiceApplication.class, args);
	}

}
