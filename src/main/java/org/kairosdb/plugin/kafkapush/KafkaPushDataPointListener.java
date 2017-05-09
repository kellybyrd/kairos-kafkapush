package org.kairosdb.plugin.kafkapush;

import com.google.common.collect.ImmutableMap;

import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONException;
import org.json.JSONWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kairosdb.core.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.SortedMap;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;


public class KafkaPushDataPointListener implements org.kairosdb.core.DataPointListener
{
    public static final Logger logger = LoggerFactory.getLogger(KafkaPushDataPointListener.class);
    private static final Map<String, String> REPLACE_CHAR_MAP = ImmutableMap.of(
            "dash",       "-",
            "dot",        ".",
            "underscore", "_"
    );

    private static final Pattern VAILD_TOPIC_REGEX = Pattern.compile("[^a-zA-Z0-9-]");


    private Producer<String, String> kafkaClient;
    private String kafkaDefaultTopic;
    private Optional<String> kafkaTopicTag;
    private String kafkaTopicReplaceChar;

    @Inject
    public KafkaPushDataPointListener(Properties props) throws IllegalArgumentException
    {
        this(props, configureKafkaProducer(props));
    }

    public KafkaPushDataPointListener(Properties props, Producer kafkaClient) throws IllegalArgumentException
    {
        this.kafkaClient = kafkaClient;
        // topic.default is required
        kafkaDefaultTopic = StringUtils.trimToEmpty(props.getProperty("kairosdb.kafkapush.topic.default", ""));
        checkArgument(StringUtils.isNotBlank(kafkaDefaultTopic));

        // Properties with defaults
        kafkaTopicTag = Optional.ofNullable(props.getProperty("kairosdb.kafkapush.topic.tag", ""));
        kafkaTopicReplaceChar =
                REPLACE_CHAR_MAP.getOrDefault(StringUtils.trimToEmpty(
                        props.getProperty("kairosdb.kafkapush.topic.replacechar")),"_");
        logger.info("Created datapoint listener.");
    }

    @Override
    public void dataPoint(String metricName, SortedMap<String, String> tags, DataPoint dataPoint)
    {
        Optional<String> curTopic = getKafkaTopicName(tags);
        if (curTopic.isPresent()) {
            makeJsonMessage(metricName, tags, dataPoint).ifPresent(
                    json -> kafkaClient.send(new ProducerRecord<>(curTopic.get(), metricName,
                            json)));
        } else {
            // We should never see this because the constructor should enforce a default topic
            // being set.
            logger.error("Could not produce valid kafka topic, not sending datapoint.");
        }
    }

    public String getKafkaDefaultTopic() {
        return kafkaDefaultTopic;
    }

    public String getKafkaTopicReplaceChar() {
        return kafkaTopicReplaceChar;
    }

    public Optional<String> getKafkaTopicTag() {
        return kafkaTopicTag;
    }

    /*
     * The JSON format is done for ease of ingestion into a Riemann system.
     */
    private Optional<String> makeJsonMessage(String metricName, SortedMap<String, String> tags,
            DataPoint dataPoint)
    {
        Optional<String> ret = Optional.empty();

        StringWriter stringWriter = new StringWriter();
        JSONWriter jsonWriter = new JSONWriter(stringWriter);
        try {
            jsonWriter.object();
            jsonWriter.key("service").value(metricName);

            if (tags.containsKey("host")) {
                jsonWriter.key("host").value(tags.get("host"));
            }

            jsonWriter.key("metric");
            if (dataPoint.isLong()) {
                jsonWriter.value(dataPoint.getLongValue());
            }
            else if (dataPoint.isDouble()) {
                jsonWriter.value(dataPoint.getDoubleValue());
            }
            else {
                //String or complex values can't be fit into "metric", they'll only show up in the
                // "datapoint" attribute.
                jsonWriter.value(null);
            }

            jsonWriter.key("datapoint").object();
            jsonWriter.key("timestamp").value(dataPoint.getTimestamp());
            jsonWriter.key("value");
            dataPoint.writeValueToJson(jsonWriter);
            jsonWriter.key("type").value(dataPoint.getApiDataType());
            jsonWriter.endObject();

            jsonWriter.key("kariosdb_tags").value(tags);
            jsonWriter.endObject();

            ret = Optional.of(stringWriter.toString());
        } catch (JSONException e) {
            logger.error("Error assembling JSON for output to Kafka: {}", e.getMessage());
            logger.error("DataPoint was: {}", dataPoint);
            logger.error("JSON in progress was: {}", stringWriter.toString());
            logger.error("Stacktrace: {}", e.getStackTrace().toString());
        }

        return ret;
    }

    /*
     * Given a DataPoint, return the correct topic. Right now the logic is:
     * - If 'kairosdb.kafkapush.topic.tag' is not blank and that tag exists in current
     *      datapoint, use the value of that tag.
     * - Otherwise use the value of kairosdb.kafkapush.topic.default
     *
     * NOTES:
     * - This function will normalize whatever result it gets to produce a valid Kafka
     *   topic name. If the normalized result is blank, it will return Optional.empty.
     */
    private Optional<String> getKafkaTopicName(final SortedMap<String, String> tags)
    {
        return kafkaTopicTag.map(s -> {
            String topic = StringUtils.trimToEmpty(
                    StringUtils.defaultIfBlank(tags.get(s), kafkaDefaultTopic));
            return VAILD_TOPIC_REGEX.matcher(topic).replaceAll(kafkaTopicReplaceChar);
        }).filter(StringUtils::isNotBlank);
    }

    static private Producer configureKafkaProducer(Properties kairosProps) throws IllegalArgumentException
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", kairosProps.getProperty("kairosdb.kafkapush.servers", ""));

        props.put("acks", kairosProps.getProperty("kairosdb.kafkapush.acks", "all"));
        props.put("retries", NumberUtils.toInt(kairosProps.getProperty("kairosdb.kafkapush.retries"), 0));
        props.put("linger.ms", NumberUtils.toInt(kairosProps.getProperty("kairosdb.kafkapush.linger"), 1));
        props.put("batch.size", NumberUtils.toInt(kairosProps.getProperty("kairosdb.kafkapush.batch_size"), 16384));
        props.put("buffer.memory", NumberUtils.toInt(kairosProps.getProperty("kairosdb.kafkapush.buffer_memory"), 3554432));

        try {
            return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        }
        catch (KafkaException ke) {
            logger.error("Couldn't configure KafkaProducer: {}", ke);
            throw new IllegalArgumentException(ke);
        }
    }
}

