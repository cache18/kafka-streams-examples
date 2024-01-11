package com.course.kafka.broker.stream.customer.preference;

import com.course.kafka.broker.message.CustomerPreferenceAggregateMessage;
import com.course.kafka.broker.message.CustomerPreferenceShoppingCartMessage;
import com.course.kafka.broker.message.CustomerPreferenceWishlistMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class CustomerPreferenceOneStream {

    private static final CustomerPreferenceShoppingCardAggregator SHOPPING_CARD_AGGREGATOR
             = new CustomerPreferenceShoppingCardAggregator();

    private static final CustomerPreferenceWishlistCardAggregator WISHLIST_CARD_AGGREGATOR
            = new CustomerPreferenceWishlistCardAggregator();

    @Bean
    public KStream<String, CustomerPreferenceAggregateMessage> kStreamCustomerPreferenceAll(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var shoppingCardSerde = new JsonSerde<>(CustomerPreferenceShoppingCartMessage.class);
        var wishlistCardSerde = new JsonSerde<>(CustomerPreferenceWishlistMessage.class);
        var aggregateSerde = new JsonSerde<>(CustomerPreferenceAggregateMessage.class);

        KGroupedStream<String, CustomerPreferenceShoppingCartMessage> groupedShoppingCardStream = builder
                .stream("t-commodity-customer-preference-shopping-cart", Consumed.with(stringSerde, shoppingCardSerde))
                .groupByKey();

        KGroupedStream<String, CustomerPreferenceWishlistMessage> groupedWishlistCardStream = builder
                .stream("t-commodity-customer-preference-wishlist", Consumed.with(stringSerde, wishlistCardSerde))
                .groupByKey();

        var customerPreferenceStream = groupedShoppingCardStream
                .cogroup(SHOPPING_CARD_AGGREGATOR)
                .cogroup(groupedWishlistCardStream, WISHLIST_CARD_AGGREGATOR)
                .aggregate(CustomerPreferenceAggregateMessage::new, Materialized.with(stringSerde, aggregateSerde))
                .toStream();

        customerPreferenceStream.to("t-commodity-customer-preference-all", Produced.with(stringSerde,  aggregateSerde));

        return customerPreferenceStream;
    }
}
