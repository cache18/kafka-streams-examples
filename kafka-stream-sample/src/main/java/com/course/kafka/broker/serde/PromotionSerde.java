package com.course.kafka.broker.serde;

import com.course.kafka.broker.message.PromotionMessage;

public class PromotionSerde extends CustomJsonSerde<PromotionMessage> {
    public PromotionSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(PromotionMessage.class));
    }
}
