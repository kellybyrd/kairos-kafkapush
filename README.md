kairos-kafkapush
============

Kafka producer plugin for KairosDB. This plugin registers a DataPointListener and pushes new datapoints to a Kafka cluster.

Currently built against KairosDB 0.9.4 and JDK 1.8


Using the Kafka Plugin
----------------------

To use this:

1. Build the jar with 'mvn package'
1. Copy copy the jar and karios-kafkapush.properties files into /opt/kairosdb/lib and
/opt/kairosdb/conf directories respectively.
1. In the properties file, rdit at least kairosdb.kafkapush.server, kairosdb.kafkapush.port, and
kairosdb.kafkapush.topic properties for your local environment


Using the Kafka Plugin
----------------------
This initial version pushes KariosDB datapoints to Kafka with metric_name as the key, and then
a JSON objecct as the value. The format is skewed for easy ingestion into Riemman.
```
{
  "service": $METRIC NAME
  "host": $HOST_TAG,                  // Only present if a host tag exists in the datapoint.
  "metric": $DATAPOINT_VALUE          // If datapoint type is long or double, value goes here.
                                      //    else 'null'
  "datapoint":{
        "timestamp": 1493681535308,
        "value": 42,
        "type": "long"
  }
  "kairosdb_tags": {
        "foo": "bar",
        "host":"machine.lan.net"
  },
}
```


Information about Kairos plugins
--------------------------------

For information on how KairosDB plugins work see the
[project page](https://kairosdb.github.io/docs/build/html/kairosdevelopment/Plugins.html)


Todo
-----------
* Unit tests
* Better packaging, install, etc
* Everything else
