package org.kairosdb.plugin.kafkapush;

import com.google.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.json.JSONWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kairosdb.core.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Properties;
import java.util.SortedMap;

import static com.google.common.base.Preconditions.checkArgument;

public class KafkaPushDataPointListener implements org.kairosdb.core.DataPointListener
{
    public static final Logger logger = LoggerFactory.getLogger(KafkaPushDataPointListener.class);

    private Producer<String, String> kafkaClient;
    private String kafkaTopic;

    @Inject
    public KafkaPushDataPointListener(Properties kairos_props)
    {
        Properties props = new Properties();

        // Required properties
        String kafkaServer = kairos_props.getProperty("kairosdb.kafkapush.server", "");
        short kafkaPort = NumberUtils.toShort(kairos_props.getProperty("kairosdb.kafkapush.port"),NumberUtils.SHORT_ZERO);
        kafkaTopic = kairos_props.getProperty("kairosdb.kafkapush.topic", "");

        checkArgument(StringUtils.isNotBlank(kafkaServer), "Kafka server name must not be blank");
        checkArgument(kafkaPort > 0, "Kafka port number must be > 0 ");
        checkArgument(StringUtils.isNotBlank(kafkaTopic), "Kafka topic must not be blank");

        props.put("bootstrap.servers", kafkaServer + ':' + Integer.toString(kafkaPort));

        // Properties with defaults
        props.put("acks",                             kairos_props.getProperty("kairosdb.kafkapush.acks",          "all"));
        props.put("retries",        NumberUtils.toInt(kairos_props.getProperty("kairosdb.kafkapush.retries"),       0));
        props.put("linger.ms",      NumberUtils.toInt(kairos_props.getProperty("kairosdb.kafkapush.linger"),        1));
        props.put("batch.size",     NumberUtils.toInt(kairos_props.getProperty("kairosdb.kafkapush.batch_size"),    16384));
        props.put("buffer.memory",  NumberUtils.toInt(kairos_props.getProperty("kairosdb.kafkapush.buffer_memory"), 3554432));

        // Not-user settable
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        kafkaClient = new KafkaProducer<>(props);
        logger.debug("Created KafkaPush DataListener with kafka bootstrap.server {}:{}", kafkaServer, kafkaPort);
    }

    @Override
    public void dataPoint(String metricName, SortedMap<String, String> tags, DataPoint dataPoint)
    {
        StringWriter stringWriter = new StringWriter();
        JSONWriter jsonWriter = new JSONWriter(stringWriter);
        try {

            jsonWriter.object();
            jsonWriter.key("name").value(metricName);
            jsonWriter.key("tags").value(tags);
            jsonWriter.key("datapoints").array();

            jsonWriter.array().value(dataPoint.getTimestamp());
            dataPoint.writeValueToJson(jsonWriter);
            jsonWriter.value(dataPoint.getApiDataType()).endArray();

            jsonWriter.endArray();
            jsonWriter.endObject();

            kafkaClient.send(new ProducerRecord<>(kafkaTopic, metricName, stringWriter.toString()));
        }
        catch (Exception e) {
            logger.error("Error assembling JSON for output to Kafka: {}", e.getMessage());
            logger.error("DataPoint was: {}", dataPoint);
            logger.error("JSON in progress was: {}", stringWriter.toString());
            logger.error("Stacktrace: {}", e.getStackTrace().toString());
        }

    }
}

