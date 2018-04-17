package com.kickstarter.dropwizard.metrics.influxdb.io;

import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import com.kickstarter.dropwizard.metrics.influxdb.InfluxDbMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

/**
 * Sends measurements to InfluxDB. Uses an {@link EvictingQueue} to store and retry measurements that have
 * failed to send, and timestamps measurements at the configured {@code precision}, up to millisecond precision.
 */
public class Sender {
  private static final Logger log = LoggerFactory.getLogger(Sender.class);

  private static final int DEFAULT_QUEUE_SIZE = 5000;
  private static final String SEPARATOR = "\n";

  private final InfluxDbWriter writer;
  private final EvictingQueue<InfluxDbMeasurement> queuedInfluxDbMeasurements;

  public Sender(final InfluxDbWriter writer) {
    this(writer, DEFAULT_QUEUE_SIZE);
  }

  public Sender(final InfluxDbWriter writer, final int queueSize) {
    this.writer = writer;
    this.queuedInfluxDbMeasurements = EvictingQueue.create(queueSize);
  }

  @VisibleForTesting int queuedMeasures() {
    return queuedInfluxDbMeasurements.size();
  }

  /**
   * Sends the provided {@link InfluxDbMeasurement measurements} to InfluxDB.
   *
   * @return true if the measurements were successfully sent.
   */
  public boolean send(final Collection<InfluxDbMeasurement> influxDbMeasurements) {
    queuedInfluxDbMeasurements.addAll(influxDbMeasurements);

    if (queuedInfluxDbMeasurements.isEmpty()) {
      return true;
    }

    final String measureLines = String.join(
      SEPARATOR,
      queuedInfluxDbMeasurements.stream()
        .map(InfluxDbMeasurement::toLine)
        .collect(toList())
    ) + SEPARATOR;

    try {
      final byte[] measureBytes = measureLines.getBytes("UTF-8");
      writer.writeBytes(measureBytes);
      queuedInfluxDbMeasurements.clear();
      return true;
    } catch (final Exception e) {
      log.warn("failed to send metrics", e);
    }

    if (queuedInfluxDbMeasurements.remainingCapacity() == 0) {
      log.warn("Queued measurements at capacity");
    }

    return false;
  }
}
