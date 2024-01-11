package com.course.kafka.broker.stream.promotion;

import com.course.kafka.broker.message.PromotionMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
public class PromotionUppercaseJsonStream {

    private static final Logger LOG = LoggerFactory.getLogger(PromotionUppercaseJsonStream.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public KStream<String, String> kstreamPromotionUppercase(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        KStream<String, String> sourceStream = builder.stream(
                "t-commodity-promotion", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> uppercaseStream = sourceStream.mapValues(this::uppercasePromotionCode);

        uppercaseStream.to("t-commodity-promotion-uppercase");

        sourceStream.print(Printed.<String, String>toSysOut().withLabel("JSON original stream"));
        uppercaseStream.print(Printed.<String, String>toSysOut().withLabel("JSON uppercase stream"));

        return sourceStream;
    }

    private String uppercasePromotionCode(String message) {
        try {
            var original = objectMapper.readValue(message, PromotionMessage.class);
            var converted = new PromotionMessage(original.getPromotionCode().toUpperCase());

            return objectMapper.writeValueAsString(converted);
        } catch (Exception e) {
            return "";
        }
    }
}
