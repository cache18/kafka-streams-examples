package com.course.kafka.broker.stream.web;

import com.course.kafka.broker.message.WebColorVoteMessage;
import com.course.kafka.broker.message.WebDesignVoteMessage;
import com.course.kafka.broker.message.WebLayoutVoteMessage;
import com.course.kafka.util.WebColorVoteTimestampExtractor;
import com.course.kafka.util.WebLayoutVoteTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class WebDesignVoteFourStream {

    @Bean
    public KStream<String, WebDesignVoteMessage> kStreamWebDesignVote(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var colorSerde = new JsonSerde<>(WebColorVoteMessage.class);
        var layoutSerde = new JsonSerde<>(WebLayoutVoteMessage.class);
        var designSerde = new JsonSerde<>(WebDesignVoteMessage.class);

        var colorTable = builder.stream("t-commodity-web-vote-color",
                        Consumed.with(stringSerde, colorSerde, new WebColorVoteTimestampExtractor(), null))
                .mapValues(WebColorVoteMessage::getColor)
                .toTable();

        var layoutTable = builder.stream("t-commodity-web-vote-layout",
                        Consumed.with(stringSerde, layoutSerde, new WebLayoutVoteTimestampExtractor(), null))
                .mapValues(WebLayoutVoteMessage::getLayout)
                .toTable();

        var joinTable = colorTable.outerJoin(layoutTable, this::voteJoiner, Materialized.with(stringSerde, designSerde));

        joinTable.toStream().to("t-commodity-web-vote-four-result");

        joinTable
                .groupBy((username, votedDesign) -> KeyValue.pair(votedDesign.getColor(), votedDesign.getColor()))
                .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("Vote four - color"));

        joinTable
                .groupBy((username, votedDesign) -> KeyValue.pair(votedDesign.getLayout(), votedDesign.getLayout()))
                .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("Vote four - layout"));

        return joinTable.toStream();
    }

    private WebDesignVoteMessage voteJoiner(String color, String layout) {
        WebDesignVoteMessage message = new WebDesignVoteMessage();
        message.setColor(color);
        message.setLayout(layout);
        return message;
    }
}
