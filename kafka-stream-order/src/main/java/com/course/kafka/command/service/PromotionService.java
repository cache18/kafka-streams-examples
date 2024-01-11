package com.course.kafka.command.service;

import com.course.kafka.api.request.PromotionRequest;
import com.course.kafka.command.action.PromotionAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PromotionService {

    @Autowired
    private PromotionAction action;

    public void createPromotion(PromotionRequest request) {
        action.sendToKafka(request);
    }
}
