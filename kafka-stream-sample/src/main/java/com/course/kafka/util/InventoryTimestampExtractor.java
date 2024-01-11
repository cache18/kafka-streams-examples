package com.course.kafka.util;

import com.course.kafka.broker.message.InventoryMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class InventoryTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var inventoryMsg = (InventoryMessage) record.value();

        return inventoryMsg != null
                ? LocalDateTimeUtil.toEpochTimestamp(inventoryMsg.getTransactionTime())
                : record.timestamp();
    }
}
