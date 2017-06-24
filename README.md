# Dropwizard InfluxDB Reporter

[![CircleCI](https://circleci.com/gh/kickstarter/dropwizard-influxdb-reporter.svg?style=svg)](https://circleci.com/gh/kickstarter/dropwizard-influxdb-reporter)

Sane Dropwizard metrics instrumentation for InfluxDB 1.2+.

```ruby
> show series
key
---
client_connections,client=influxdb-http-writer
connections,port=8080
connections,port=8081
http_server
http_server,metric=1xx-responses
http_server,metric=2xx-responses
http_server,metric=3xx-responses
http_server,metric=4xx-responses
http_server,metric=5xx-responses
http_server,metric=async-dispatches
http_server,metric=async-timeouts
http_server,metric=connect-requests
http_server,metric=delete-requests
http_server,metric=dispatches
http_server,metric=get-requests
http_server,metric=head-requests
http_server,metric=move-requests
http_server,metric=options-requests
http_server,metric=other-requests
http_server,metric=post-requests
http_server,metric=put-requests
http_server,metric=requests
http_server,metric=trace-requests
jvm
jvm_buffers
jvm_buffers,type=direct
jvm_buffers,type=mapped
jvm_classloader
jvm_gc
jvm_memory,metric=heap
jvm_memory,metric=non-heap
jvm_memory,metric=pools.Code-Cache
jvm_memory,metric=pools.Compressed-Class-Space
jvm_memory,metric=pools.Metaspace
jvm_memory,metric=pools.PS-Eden-Space
jvm_memory,metric=pools.PS-Old-Gen
jvm_memory,metric=pools.PS-Survivor-Space
jvm_memory,metric=total
jvm_threads
logging,level=all
logging,level=debug
logging,level=error
logging,level=info
logging,level=trace
logging,level=warn
resources,method=get,resource=AdminResource
thread_pools,pool=dw
```


## Installation

> Note: This library is only available on Jitpack for now; see the [Jitpack page](https://jitpack.io/#kickstarter/dropwizard-influxdb-reporter) for installation details.

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
  <groupId>com.kickstarter.dropwizard</groupId>
  <artifactId>dropwizard-influxdb-reporter</artifactId>
  <version>${dropwizard-influxdb-reporter.version}</version>
</dependency>
```

### Gradle

Add the following dependency to your gradle file:

```groovy
repositories {
  mavenCentral()
}

dependencies {
  compile 'com.kickstarter.dropwizard:dropwizard-influxdb-reporter${dropwizardInfluxdbReporterVersion}'
}
```

## Configuration

To begin scheduling metric reports at a regular interval, add a reporter to your Dropwizard config. A barebones reporter will look like this:

```yml
metrics:
  reporters:
  - type: influxdb
    sender:
      type: http
      database: mydb
```

This will send metrics to an InfluxDB instance on localhost:8086 over HTTP.

A more complex sender may look like this:

```yml
  - type: influxdb
    globalTags:
      env: production
    metricTemplates:
      services:
        pattern: com\.kickstarter\.services\.(?<service>[A-Za-z]+).*
        tagKeys: ["service"]
    groupGauges: true
    groupCounters: false
    sender:
      type: tcp
      host: localhost
      port: 90210
      timeout: 5000000 days # keep alive as long as possible
```

## Features

#### Global Tagging

Add tags to everything that gets reported.

```yml
globalTags:
  env: production
  service: recommendations
```

#### Metric grouping for Gauges and Counters

Group gauges and counters together under a single measurement. Enabled by default.

```java
<"org.eclipse.jetty.util.thread.QueuedThreadPool.dw.jobs" value=0>
<"org.eclipse.jetty.util.thread.QueuedThreadPool.dw.size" value=4>
<"org.eclipse.jetty.util.thread.QueuedThreadPool.dw.utilization" value=0.455>
<"org.eclipse.jetty.util.thread.QueuedThreadPool.dw.utilization-max" value=0.0068301848>

// => Output
name: org.eclipse.jetty.util.thread.QueuedThreadPool.dw
-------------------------------------------------------
time                    jobs    size    utilization    utilization-max
2017-06-10T10:18:00Z    0       4       0.455          0.0068301848
```

```yml
groupGauges: true
groupCounters: true
```

#### Regex-based Templating

Use custom templating to convert dropwizard-style metric names into reasonable measurement names and tag sets. By default, the reporter comes with its own templates. Thread pool metrics, for example, are renamed under `thread_pools`:

```
name: thread_pools
-------------------------------------------------------
time                    jobs    size    utilization    utilization-max
2017-06-10T10:18:00Z    0       4       0.455          0.0068301848
```

While timed Dropwizard resource metrics, for example, are grouped and tagged under `resources`:

```
resources,resource=InfoResource,method=stats
resources,resource=RecommendedProjectsResource,method=get
resources,resource=BatchRecommendedProjectsResource,method=get
```

```yml
metricTemplates:    
  services:
    pattern: com\.kickstarter\.services\.(?<service>[A-Za-z]+).*
    tagKeys: ["service"]
````                      

#### Per-metric measurement/field naming and tags

The reporter is able to deserialize custom InfluxDb-style measurements passed to it via Dropwizard's instrumentation layer. This allows you to fully customize the InfluxDB output of a particular metric through Dropwizard.

```java
final Timer restoreTimer = metricRegistry.timer(
  influxName("Recommendations", ImmutableMap.of(
    "action", "restore", 
    "model", "fun-model"
  ))
);

restoreTimer.time(...)

// => Output
name: Recommendations,action=restore,model=fun-model
-------------------------------------------------------
50-percentile  75-percentile  95-percentile  99-percentile ...
```

#### HTTP/TCP Senders

##### HTTP Sender

The HTTP sender transmits InfluxDB lines directly to the database, and must provide a database for usage. It uses a Jersey client to send metrics to InfluxDB, giving us some request-level timing measurements.

<img width="940" alt="screen shot 2017-06-10 at 9 27 31 pm" src="https://user-images.githubusercontent.com/2308368/27007506-a97c1588-4e23-11e7-8398-57f465bb42c8.png">

The jersey client can be customized using all of the options provided by [JerseyClientConfiguration](https://github.com/dropwizard/dropwizard/blob/master/dropwizard-client/src/main/java/io/dropwizard/client/JerseyClientConfiguration.java).

```yml
metrics:
  reporters:
    type: influxdb
    sender:
      type: http
      host: localhost
      port: 8086
      database: mydb
      jersey:
        connectionTimeout: 500 milliseconds
```

##### TCP Sender

You may wish to send InfluxDB lines to a collector instance, like Telegraf, instead of using the direct HTTP protocol. You can use the TCP sender to transmit metrics to your collector in InfluxDB line format.

```yml
metrics:
  reporters:
    type: influxdb
    sender:
      type: tcp
      host: localhost
      port: 8086
      timeout: 500 milliseconds
```

##### Exception Handling

A Sender sends a batch of InfluxDbMeasurements to a receiver at the Dropwizard-configured frequency. If the sender catches an exception while writing to the receiver, the exception is logged and the connection is closed. The sender will reconnect to the receiver when the next batch is scheduled to be sent.

The measurements that failed to send are stored in a queue and retried in subsequent batches. When things get **real bad™️** and the queue gets backed up, we'll start dropping old metrics — this logic is all handled by Guava's `EvictingQueue`.

## Contributing

Have questions or feedback? The best way to submit feedback and report bugs is to open a GitHub issue. We'd love to see you contribute — talk to you soon!

## License

```
Copyright 2017 Kickstarter, PBC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
