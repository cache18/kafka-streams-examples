package com.course.kafka.broker.consumer;

import com.course.kafka.broker.message.PromotionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PromotionUppercaseListener {

    private static final Logger LOG = LoggerFactory.getLogger(PromotionUppercaseListener.class);

    @KafkaListener(topics = "t-commodity-promotion-uppercase")
    public void listenPromotionUppercase(PromotionMessage message) {
        LOG.info("Processing uppercase promotion : {}", message);
    }
}
