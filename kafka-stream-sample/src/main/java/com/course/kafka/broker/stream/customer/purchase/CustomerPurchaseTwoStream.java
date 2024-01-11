package com.course.kafka.broker.stream.customer.purchase;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

//@Configuration
public class CustomerPurchaseTwoStream {

    @Bean
    public KStream<String, String> kStreamCustomerPurchaseAll(StreamsBuilder builder) {
        List<String> topics = List.of("t-commodity-customer-purchase-mobile", "t-commodity-customer-purchase-web");
        KStream<String, String> allStreams = builder.stream(topics, Consumed.with(Serdes.String(), Serdes.String()));

        allStreams.to("t-commodity-customer-purchase-all");

        return allStreams;
    }
}
