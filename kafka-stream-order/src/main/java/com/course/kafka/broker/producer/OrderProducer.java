package com.course.kafka.broker.producer;

import com.course.kafka.broker.message.OrderMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;

@Service
public class OrderProducer {

    private static final Logger LOG = LoggerFactory.getLogger(OrderProducer.class);

    @Autowired
    private KafkaTemplate<String, OrderMessage> kafkaTemplate;

    public void publish(OrderMessage message) {
        ProducerRecord<String, OrderMessage> producerRecord = buildProducerRecord(message);
        kafkaTemplate.send(producerRecord)
                .addCallback(new ListenableFutureCallback<>() {
                    @Override
                    public void onFailure(Throwable ex) {
                        LOG.info("Order {}, item {} failed to publish because {}",
                                message.getOrderNumber(), message.getItemName(), ex.getMessage());
                    }

                    @Override
                    public void onSuccess(SendResult<String, OrderMessage> result) {
                        LOG.info("Order {}, item {} published successfully",
                                message.getOrderNumber(), message.getItemName());
                    }
                });

        LOG.info("Just a dummy message for order {}, item {}", message.getOrderNumber(), message.getItemName());
    }

    private ProducerRecord<String, OrderMessage> buildProducerRecord(OrderMessage message) {
        int surpriseBonus = StringUtils.startsWithIgnoreCase(message.getOrderLocation(), "A") ? 25 : 15;
        var headers = new ArrayList<Header>();
        var surpriseBonusHeader = new RecordHeader("surpriseBonus", Integer.toString(surpriseBonus).getBytes());

        headers.add(surpriseBonusHeader);

        return new ProducerRecord<>("t-commodity-order", null, message.getOrderNumber(), message, headers);
    }
}
