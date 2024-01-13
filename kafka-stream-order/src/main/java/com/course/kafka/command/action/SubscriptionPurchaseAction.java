package com.course.kafka.command.action;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.course.kafka.api.request.SubscriptionPurchaseRequest;
import com.course.kafka.broker.message.SubscriptionPurchaseMessage;
import com.course.kafka.broker.producer.SubscriptionPurchaseProducer;

@Component
public class SubscriptionPurchaseAction {

	@Autowired
	private SubscriptionPurchaseProducer producer;

	public void publishToKafka(SubscriptionPurchaseRequest request) {
		var message = new SubscriptionPurchaseMessage();

		message.setSubscriptionNumber(request.getSubscriptionNumber());
		message.setUsername(request.getUsername());

		producer.publish(message);
	}

}
