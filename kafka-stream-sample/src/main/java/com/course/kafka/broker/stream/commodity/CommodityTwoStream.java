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
public class CommodityTwoStream {

    @Bean
    public KStream<String, OrderMessage> kStreamCommodityTrading(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var orderSerde = new JsonSerde<>(OrderMessage.class);
        var orderPatternSerde = new JsonSerde<>(OrderPatternMessage.class);
        var orderRewardSerde = new JsonSerde<>(OrderRewardMessage.class);

        KStream<String, OrderMessage> maskedCCStream = builder.stream(
                "t-commodity-order", Consumed.with(stringSerde, orderSerde))
                .mapValues(CommodityStreamUtil::maskCreditCard);

        KStream<String, OrderPatternMessage>[] patternStreams = maskedCCStream
                .mapValues(CommodityStreamUtil::mapToOrderPattern)
                .branch(CommodityStreamUtil.isPlastic(), (k, v) -> true);

        var plasticIndex = 0;
        var notPlasticIndex = 1;

        patternStreams[plasticIndex].to("t-commodity-pattern-two-plastic", Produced.with(stringSerde, orderPatternSerde));
        patternStreams[notPlasticIndex].to("t-commodity-pattern-two-notplastic", Produced.with(stringSerde, orderPatternSerde));

        KStream<String, OrderRewardMessage> rewardStream = maskedCCStream
                .filter(CommodityStreamUtil.isLargeQuantity())
                .filterNot(CommodityStreamUtil.isCheap())
                .mapValues(CommodityStreamUtil::mapToOrderReward);
        rewardStream.to("t-commodity-reward-two", Produced.with(stringSerde, orderRewardSerde));

        KStream<String, OrderMessage> storageStream = maskedCCStream.selectKey(CommodityStreamUtil.generateStorageKey());
        storageStream.to("t-commodity-storage-two", Produced.with(stringSerde, orderSerde));

        return maskedCCStream;
    }
}
