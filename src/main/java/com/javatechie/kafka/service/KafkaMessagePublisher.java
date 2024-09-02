package com.javatechie.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.javatechie.kafka.dto.Customer;

@Service
public class KafkaMessagePublisher {
	
	@Autowired
	private KafkaTemplate<String, Object> template;
	
	public void sendMessage(String message) {
		ListenableFuture<SendResult<String, Object>> future = template.send("javatechie-topic1", message);
		
		future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {

			@Override
			public void onSuccess(SendResult<String, Object> result) {
				System.out.println("Message sent to partition - "+result.getRecordMetadata().partition()
						+ "with offset - "+result.getRecordMetadata().offset());
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to send message - "+ex.getMessage());
			}
			
		});
	}
	
	public void sendEvent(Customer customer) {
		System.out.println("in sendevent service");
		ListenableFuture<SendResult<String, Object>> future = template.send("javatechie-topic2", customer);
		
		future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {

			@Override
			public void onSuccess(SendResult<String, Object> result) {
				System.out.println("Event sent => "+customer.toString());
				System.out.println("Event sent to partition - "+result.getRecordMetadata().partition()
						+ "with offset - "+result.getRecordMetadata().offset());
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to send event - "+ex.getMessage());
			}
			
		});
	}
}
