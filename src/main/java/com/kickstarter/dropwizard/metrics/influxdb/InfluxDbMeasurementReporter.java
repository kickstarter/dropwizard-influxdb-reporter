package com.kickstarter.dropwizard.metrics.influxdb;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.kickstarter.dropwizard.metrics.influxdb.io.Sender;
import com.kickstarter.dropwizard.metrics.influxdb.transformer.DropwizardMeasurement;
import com.kickstarter.dropwizard.metrics.influxdb.transformer.DropwizardTransformer;

import java.time.Clock;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A Dropwizard {@link ScheduledReporter} for reporting measurements in InfluxDB line format.
 * <p>
 * <p>Supports global tags, tagged templating, counter/gauge grouping,
 * and per-metric tagging via {@link DropwizardMeasurement#toString}.
 */
public class InfluxDbMeasurementReporter extends ScheduledReporter {
  private final Clock clock;
  private final Sender sender;
  private final DropwizardTransformer transformer;

  public InfluxDbMeasurementReporter(final Sender sender,
                                     final MetricRegistry registry,
                                     final MetricFilter filter,
                                     final TimeUnit rateUnit,
                                     final TimeUnit durationUnit,
                                     final Clock clock,
                                     final DropwizardTransformer transformer,
                                     final ScheduledExecutorService executor) {
    super(registry, "influxdb-measurement-reporter", filter, rateUnit, durationUnit, executor);
    this.clock = clock;
    this.sender = sender;
    this.transformer = transformer;
  }

  public InfluxDbMeasurementReporter(final Sender sender,
                                     final MetricRegistry registry,
                                     final MetricFilter filter,
                                     final TimeUnit rateUnit,
                                     final TimeUnit durationUnit,
                                     final Clock clock,
                                     final DropwizardTransformer transformer) {
    super(registry, "influxdb-measurement-reporter", filter, rateUnit, durationUnit);
    this.clock = clock;
    this.sender = sender;
    this.transformer = transformer;
  }

  @Override
  public void report(final SortedMap<String, Gauge> gauges,
                     final SortedMap<String, Counter> counters,
                     final SortedMap<String, Histogram> histograms,
                     final SortedMap<String, Meter> meters,
                     final SortedMap<String, Timer> timers) {
    final long timestamp = clock.instant().toEpochMilli();
    final ImmutableList<InfluxDbMeasurement> influxDbMeasurements = ImmutableList.<InfluxDbMeasurement>builder()
      .addAll(transformer.fromGauges(gauges, timestamp))
      .addAll(transformer.fromCounters(counters, timestamp))
      .addAll(transformer.fromHistograms(histograms, timestamp))
      .addAll(transformer.fromMeters(meters, timestamp))
      .addAll(transformer.fromTimers(timers, timestamp))
      .build();

    sender.send(influxDbMeasurements);
  }
}
