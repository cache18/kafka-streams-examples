package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

//@Configuration
public class FeedbackFourStream {
    private static final Set<String> GOOD_WORDS = Set.of("happy", "good", "helpful");

    private static final Set<String> BAD_WORDS = Set.of("angry", "sad", "bad");

    private Predicate<String, String> isBadWord() {
        return (key, value) -> BAD_WORDS.contains(value);
    }

    private Predicate<String, String> isGoodWord() {
        return (key, value) -> GOOD_WORDS.contains(value);
    }

    @Bean
    public KStream<String, FeedbackMessage> kStreamFeedback(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

        var sourceStream = builder.stream("t-commodity-feedback", Consumed.with(stringSerde, feedbackSerde));

        sourceStream
                .flatMap(splitWords())
                .split()
                .branch(
                        isGoodWord(),
                        Branched.withConsumer(ks -> {
                            ks.to("t-commodity-feedback-four-good");
                            ks.groupByKey().count().toStream().to("t-commodity-feedback-four-good-count");
                        }))
                .branch(
                        isBadWord(),
                        Branched.withConsumer(ks -> {
                            ks.to("t-commodity-feedback-four-bad");
                            ks.groupByKey().count().toStream().to("t-commodity-feedback-four-bad-count");
                        }));

        return sourceStream;
    }

    private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String, String>>> splitWords() {
        return (key, value) -> Arrays.stream(value
                        .getFeedback()
                        .replaceAll("[^a-zA-Z ]", "")
                        .toLowerCase()
                        .split("\\s+"))
                .distinct()
                .map(word -> KeyValue.pair(value.getLocation(), word))
                .collect(Collectors.toList());
    }

}
