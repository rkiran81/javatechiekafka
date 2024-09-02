package com.javatechie.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.javatechie.kafka.dto.Customer;
import com.javatechie.kafka.service.KafkaMessagePublisher;

@RestController()
public class EventController {
	
	@Autowired
	private KafkaMessagePublisher publisher;
	
	@GetMapping("/test")
	public void test() {
		System.out.println("Test Successfull");
	}
	
	@GetMapping("/publish/{message}")
	public ResponseEntity<?> sendMessage(@PathVariable String message) {
		try {
			publisher.sendMessage(message);
			return ResponseEntity.ok("Message published successfully...");
		}catch(Exception ex) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	@PostMapping("/publish/event")
	public ResponseEntity<?> sendEvent(@RequestBody Customer customer) {
		System.out.println("In Controller");
		try {
			publisher.sendEvent(customer);
			return ResponseEntity.ok("Message published successfully...");
		}catch(Exception ex) {
			System.out.println("Exception sending event => "+ex.getMessage());
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}
}
