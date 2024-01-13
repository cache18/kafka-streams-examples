package com.course.kafka.command.action;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.course.kafka.api.request.SubscriptionUserRequest;
import com.course.kafka.broker.message.SubscriptionUserMessage;
import com.course.kafka.broker.producer.SubscriptionUserProducer;

@Component
public class SubscriptionUserAction {

	@Autowired
	private SubscriptionUserProducer producer;

	public void publishToKafka(SubscriptionUserRequest request) {
		var message = new SubscriptionUserMessage();

		message.setDuration(request.getDuration());
		message.setUsername(request.getUsername());

		producer.publish(message);
	}

}
