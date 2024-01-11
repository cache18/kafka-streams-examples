package com.course.kafka.broker.stream.customer.purchase;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
public class CustomerPurchaseOneStream {

    @Bean
    public KStream<String, String> kStreamCustomerPurchaseAll(StreamsBuilder builder) {
        KStream<String, String> mobileStream = builder.stream(
                "t-commodity-customer-purchase-mobile",
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> webStream = builder.stream(
                "t-commodity-customer-purchase-web",
                Consumed.with(Serdes.String(), Serdes.String()));

        mobileStream.merge(webStream).to("t-commodity-customer-purchase-all");

        return mobileStream;
    }
}
