package org.kairosdb.plugin.kafkapush;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;


public class KafkaPushInitialSetupTest
{
    @Test(expected = IllegalArgumentException.class)
    public void propertiesDefaultTopicMissing()
    {
        Properties testProps = new Properties();
        new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void propertiesDefaultTopicEmpty()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "");
        new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void propertiesDefaultTopicBlank()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "   ");
        new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));
    }

    @Test
    public void propertiesDefaultTopicTrimmed()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "   test");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));

        assertEquals("test", testListner.getKafkaDefaultTopic());
    }

    @Test
    public void propertiesTopicReplaceCharMissing()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "test");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));

        assertEquals("_", testListner.getKafkaTopicReplaceChar());
    }

    @Test
    public void propertiesTopicReplaceCharUnderscore()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "test");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", "underscore ");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));

        assertEquals("_", testListner.getKafkaTopicReplaceChar());
    }

    @Test
    public void propertiesTopicReplaceCharDot()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "test");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", "   dot ");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));

        assertEquals(".", testListner.getKafkaTopicReplaceChar());
    }

    @Test
    public void propertiesTopicReplaceCharDash()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "test");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", "   dash");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));

        assertEquals("-", testListner.getKafkaTopicReplaceChar());
    }

    @Test
    public void propertiesTopicReplaceCharInvalid()
    {
        Properties testProps = new Properties();
        testProps.setProperty("kairosdb.kafkapush.topic.default", "test");
        testProps.setProperty("kairosdb.kafkapush.topic.replacechar", " not_a_valid_thing  ");
        KafkaPushDataPointListener testListner =
                new KafkaPushDataPointListener(testProps, new MockProducer<>(true, new StringSerializer(), new StringSerializer()));

        assertEquals("_", testListner.getKafkaTopicReplaceChar());
    }
}
