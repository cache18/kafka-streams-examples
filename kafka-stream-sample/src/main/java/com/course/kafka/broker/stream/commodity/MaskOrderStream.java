package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class MaskOrderStream {

    @Bean
    public KStream<String, OrderMessage> kStreamCommodityTrading(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<OrderMessage> jsonSerde = new JsonSerde<>(OrderMessage.class);
        KStream<String, OrderMessage> maskedCCStream = builder.stream(
                "t-commodity-order", Consumed.with(stringSerde, jsonSerde))
                .mapValues(CommodityStreamUtil::maskCreditCard);

        maskedCCStream.to("t-commodity-order-masked", Produced.with(stringSerde, jsonSerde));

        maskedCCStream.print(Printed.toSysOut());

        return maskedCCStream;
    }
}
