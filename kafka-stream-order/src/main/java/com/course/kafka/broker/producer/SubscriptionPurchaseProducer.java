package com.course.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.course.kafka.broker.message.SubscriptionPurchaseMessage;

@Service
public class SubscriptionPurchaseProducer {

	@Autowired
	private KafkaTemplate<String, SubscriptionPurchaseMessage> kafkaTemplate;

	public void publish(SubscriptionPurchaseMessage message) {
		kafkaTemplate.send("t-commodity-subscription-purchase", message.getUsername(), message);
	}

}
