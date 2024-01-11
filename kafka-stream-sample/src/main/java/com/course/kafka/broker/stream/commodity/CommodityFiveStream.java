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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class CommodityFiveStream {

    private static final Logger LOG = LoggerFactory.getLogger(CommodityFiveStream.class);

    @Bean
    public KStream<String, OrderMessage> kStreamCommodityTrading(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var orderSerde = new JsonSerde<>(OrderMessage.class);
        var orderPatternSerde = new JsonSerde<>(OrderPatternMessage.class);
        var orderRewardSerde = new JsonSerde<>(OrderRewardMessage.class);

        KStream<String, OrderMessage> maskedCCStream = builder.stream(
                "t-commodity-order", Consumed.with(stringSerde, orderSerde))
                .mapValues(CommodityStreamUtil::maskCreditCard);

        var branchProducer = Produced.with(stringSerde, orderPatternSerde);

        new KafkaStreamBrancher<String, OrderPatternMessage>()
                .branch(CommodityStreamUtil.isPlastic(), kstream -> kstream.to("t-commodity-pattern-five-plastic", branchProducer))
                .defaultBranch(kstream -> kstream.to("t-commodity-pattern-five-notplastic", branchProducer))
                .onTopOf(maskedCCStream.mapValues(CommodityStreamUtil::mapToOrderPattern));

        KStream<String, OrderRewardMessage> rewardStream = maskedCCStream
                .filter(CommodityStreamUtil.isLargeQuantity())
                .filterNot(CommodityStreamUtil.isCheap())
                .map(CommodityStreamUtil.mapToOrderRewardChangeKey());
        rewardStream.to("t-commodity-reward-five", Produced.with(stringSerde, orderRewardSerde));

        KStream<String, OrderMessage> storageStream = maskedCCStream.selectKey(CommodityStreamUtil.generateStorageKey());
        storageStream.to("t-commodity-storage-five", Produced.with(stringSerde, orderSerde));

        maskedCCStream
                .filter((k, v) -> v.getOrderLocation().toUpperCase().startsWith("C"))
                .foreach((k, v) -> this.reportFraud(v));

        return maskedCCStream;
    }

    private void reportFraud(OrderMessage v) {
        LOG.info("Reporting fraud : {}", v);
    }
}
