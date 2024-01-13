package com.course.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.course.kafka.broker.message.SubscriptionUserMessage;

@Service
public class SubscriptionUserProducer {

	@Autowired
	private KafkaTemplate<String, SubscriptionUserMessage> kafkaTemplate;

	public void publish(SubscriptionUserMessage message) {
		kafkaTemplate.send("t-commodity-subscription-user", message.getUsername(), message);
	}

}
