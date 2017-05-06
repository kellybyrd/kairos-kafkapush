package org.kairosdb.plugin.kafkapush;

import com.google.common.collect.ImmutableSortedMap;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datapoints.LongDataPoint;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;


public class KafkaPushTopicTest
{
    private ImmutableSortedMap<String, String> testTags;
    private long now;
    private MockProducer<String, String> mockKafka;

    @Before
    public void setup(){
        now = System.currentTimeMillis();

        testTags = ImmutableSortedMap.<String, String>naturalOrder()
                .put("tag_valid", "foo")
                .put("tag_empty", "")
                .put("tag_blank", "   ")
                .put("tag_invalid", "#abcd^&$@ -._  ")
                .build();

        mockKafka = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    }

    @Test
    public void validTopicTagInMessage()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "default");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", "dot");
        testProps.setProperty("kairosdb.kafkapush.topic.tag", "tag_valid");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, mockKafka);

        testListner.dataPoint("metric.name",testTags, new LongDataPoint(now, 1));
        assertTrue(checkMessageTopic("foo"));
    }

    @Test
    public void emptyTopicTagInMessage()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "default");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", "dot");
        testProps.setProperty("kairosdb.kafkapush.topic.tag", "tag_empty");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, mockKafka);

        testListner.dataPoint("metric.name",testTags, new LongDataPoint(now, 1));
        assertTrue(checkMessageTopic("default"));
    }

    @Test
    public void blankTopicTagInMessage()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "default");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", "dot");
        testProps.setProperty("kairosdb.kafkapush.topic.tag", "tag_blank");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, mockKafka);

        testListner.dataPoint("metric.name",testTags, new LongDataPoint(now, 1));
        assertTrue(checkMessageTopic("default"));
    }

    @Test
    public void replaceCharsInTopicTag()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "default");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", "dot");
        testProps.setProperty("kairosdb.kafkapush.topic.tag", "tag_invalid");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, mockKafka);

        testListner.dataPoint("metric.name",testTags, new LongDataPoint(now, 1));
        assertTrue(checkMessageTopic(".abcd.....-.."));
    }

    @Test
    public void replaceCharsInDefaultTopic()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", " Test_4.Topic!");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", "dash");
        testProps.setProperty("kairosdb.kafkapush.topic.tag", "tag_missing");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, mockKafka);

        testListner.dataPoint("metric.name",testTags, new LongDataPoint(now, 1));
        assertTrue(checkMessageTopic("Test-4-Topic-"));
    }

    private boolean checkMessageTopic(String testTopic)
    {
        List<ProducerRecord<String, String>> results = mockKafka.history();
        mockKafka.clear();
        return results.size() == 1 &&
                testTopic.equals(results.get(0).topic());
    }
}
