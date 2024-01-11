package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderPatternMessage;
import com.course.kafka.broker.message.OrderRewardMessage;
import com.course.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class CommodityOneStream {

    @Bean
    public KStream<String, OrderMessage> kStreamCommodityTrading(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var orderSerde = new JsonSerde<>(OrderMessage.class);
        var orderPatternSerde = new JsonSerde<>(OrderPatternMessage.class);
        var orderRewardSerde = new JsonSerde<>(OrderRewardMessage.class);

        KStream<String, OrderMessage> maskedCCStream = builder.stream(
                "t-commodity-order", Consumed.with(stringSerde, orderSerde))
                .mapValues(CommodityStreamUtil::maskCreditCard);

        KStream<String, OrderPatternMessage> patternStream = maskedCCStream.mapValues(
                CommodityStreamUtil::mapToOrderPattern);
        patternStream.to("t-commodity-pattern-one", Produced.with(stringSerde, orderPatternSerde));

        KStream<String, OrderRewardMessage> rewardStream = maskedCCStream.filter(CommodityStreamUtil.isLargeQuantity())
                .mapValues(CommodityStreamUtil::mapToOrderReward);
        rewardStream.to("t-commodity-reward-one", Produced.with(stringSerde, orderRewardSerde));

        maskedCCStream.to("t-commodity-storage-one", Produced.with(stringSerde, orderSerde));

        return maskedCCStream;
    }
}
