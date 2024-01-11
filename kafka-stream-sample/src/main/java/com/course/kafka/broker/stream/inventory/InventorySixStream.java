package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import com.course.kafka.util.InventoryTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

//@Configuration
public class InventorySixStream {

    @Bean
    public KStream<String, InventoryMessage> kStreamInventory(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var inventorySerde = new JsonSerde<>(InventoryMessage.class);
        var inventoryTimestampExtractor = new InventoryTimestampExtractor();
        var longSerde = Serdes.Long();

        var windowLength = Duration.ofHours(1L);
        var hopLength = Duration.ofMinutes(20L);
        var windowSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength.toMillis());

        var inventoryStream = builder.stream("t-commodity-inventory",
                Consumed.with(stringSerde, inventorySerde, inventoryTimestampExtractor, null));

        inventoryStream
                .mapValues((k, v) -> v.getType().equalsIgnoreCase("ADD") ? v.getQuantity() : (-1 * v.getQuantity()))
                .groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(windowLength).advanceBy(hopLength))
                .reduce(Long::sum, Materialized.with(stringSerde, longSerde)).toStream()
                .through("t-commodity-inventory-six", Produced.with(windowSerde, longSerde)).print(Printed.toSysOut());

        return inventoryStream;
    }

}