package com.kickstarter.dropwizard.metrics.influxdb;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.kickstarter.dropwizard.metrics.influxdb.io.Sender;
import com.kickstarter.dropwizard.metrics.influxdb.transformer.DropwizardTransformer;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InfluxDbMeasurementReporterTest {
  private final Clock clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"));

  @Test
  public void testReport() {
    final Sender sender = mock(Sender.class);
    final DropwizardTransformer transformer = mock(DropwizardTransformer.class);

    final InfluxDbMeasurementReporter reporter = new InfluxDbMeasurementReporter(
      sender,
      new MetricRegistry(),
      MetricFilter.ALL,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS,
      clock,
      transformer
    );

    final long timestamp = clock.instant().toEpochMilli();
    final SortedMap<String, Gauge> gauges = ImmutableSortedMap.of("some", () -> 5);
    final SortedMap<String, Counter> counters = ImmutableSortedMap.of("more", new Counter());
    final SortedMap<String, Histogram> histograms = ImmutableSortedMap.of("metrics", new Histogram(new ExponentiallyDecayingReservoir()));
    final SortedMap<String, Meter> meters = ImmutableSortedMap.of("for", new Meter());
    final SortedMap<String, Timer> timers = ImmutableSortedMap.of("for", new Timer());

    final List<InfluxDbMeasurement> expectedMeasurements =
      Stream
        .of("gauge", "counter", "histogram", "meter", "timer")
        .map(name ->
          InfluxDbMeasurement.create(
            "some",
            ImmutableMap.of(),
            ImmutableMap.of(name, "stuff"),
            timestamp
          )
        ).collect(toList());

    when(transformer.fromGauges(gauges, timestamp))
      .thenReturn(ImmutableList.of(expectedMeasurements.get(0)));

    when(transformer.fromCounters(counters, timestamp))
      .thenReturn(ImmutableList.of(expectedMeasurements.get(1)));

    when(transformer.fromHistograms(histograms, timestamp))
      .thenReturn(ImmutableList.of(expectedMeasurements.get(2)));

    when(transformer.fromMeters(meters, timestamp))
      .thenReturn(ImmutableList.of(expectedMeasurements.get(3)));

    when(transformer.fromTimers(timers, timestamp))
      .thenReturn(ImmutableList.of(expectedMeasurements.get(4)));

    reporter.report(gauges, counters, histograms, meters, timers);
    verify(sender).send(expectedMeasurements);
  }
}
