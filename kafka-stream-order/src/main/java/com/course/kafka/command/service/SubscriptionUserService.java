package com.course.kafka.command.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.course.kafka.api.request.SubscriptionUserRequest;
import com.course.kafka.command.action.SubscriptionUserAction;

@Service
public class SubscriptionUserService {

	@Autowired
	private SubscriptionUserAction action;

	public void createUser(SubscriptionUserRequest request) {
		action.publishToKafka(request);
	}

}
