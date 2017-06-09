package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.kickstarter.dropwizard.metrics.influxdb.InfluxDbMeasurement;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DropwizardTransformerTest {
  @Test
  public void testConvertDuration_Milliseconds() {
    final DropwizardTransformer transformer = transformerWithUnits(TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    assertEquals("Should convert duration to milliseconds precisely", 2.0E-5, transformer.convertDuration(20), 0.0);
  }

  @Test
  public void testConvertDuration_Seconds() {
    final DropwizardTransformer transformer = transformerWithUnits(TimeUnit.SECONDS, TimeUnit.SECONDS);
    assertEquals("Should convert duration to seconds precisely", 2.0E-8, transformer.convertDuration(20), 0.0);
  }

  @Test
  public void testConvertRate_Seconds() {
    final DropwizardTransformer transformer = transformerWithUnits(TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    assertEquals("Should convert rate to seconds precisely", 20.0, transformer.convertRate(20), 0.0);
  }

  @Test
  public void testConvertRate_Minutes() {
    final DropwizardTransformer transformer = transformerWithUnits(TimeUnit.MINUTES, TimeUnit.MILLISECONDS);
    assertEquals("Should convert rate to minutes precisely", 1200.0, transformer.convertRate(20), 0.0);
  }

  @Test
  public void testFromTimer() {
    final Set<String> fieldKeys = ImmutableSet.of(
      "count",
      "min",
      "max",
      "mean",
      "std-dev",
      "50-percentile",
      "75-percentile",
      "95-percentile",
      "99-percentile",
      "999-percentile",
      "one-minute",
      "five-minute",
      "fifteen-minute",
      "mean-minute",
      "run-count"
    );
    
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    when(parser.parse("some.metric.name")).thenReturn(
      DropwizardMeasurement.create("Measurement", MEASUREMENT_TAGS, Optional.empty())
    );

    final Timer timer = new Timer();
    timer.update(50, TimeUnit.MILLISECONDS);
    timer.update(70, TimeUnit.MILLISECONDS);
    timer.update(100, TimeUnit.MILLISECONDS);
    
    final InfluxDbMeasurement measurement = transformer.fromTimer("some.metric.name", timer, 90210L);
    assertEquals("should parse name from full metric key", "Measurement", measurement.name());
    assertEquals("should add global and measurement tags", ALL_TAGS, measurement.tags());
    assertEquals("should timestamp measurement", 90210L, measurement.timestamp());
    assertEquals("should add all timer fields", fieldKeys, measurement.fields().keySet());
  }

  @Test
  public void testFromMeter() {
    final Set<String> fieldKeys = ImmutableSet.of(
      "count",
      "one-minute",
      "five-minute",
      "fifteen-minute",
      "mean-minute"
    );

    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    when(parser.parse("some.metric.name")).thenReturn(
      DropwizardMeasurement.create("Measurement", MEASUREMENT_TAGS, Optional.empty())
    );

    final Meter meter = new Meter();
    meter.mark(50L);
    meter.mark(64L);
    meter.mark(80L);

    final InfluxDbMeasurement measurement = transformer.fromMeter("some.metric.name", meter, 90210L);
    assertEquals("should parse name from full metric key", "Measurement", measurement.name());
    assertEquals("should add global and measurement tags", ALL_TAGS, measurement.tags());
    assertEquals("should timestamp measurement", 90210L, measurement.timestamp());
    assertEquals("should add all meter fields", fieldKeys, measurement.fields().keySet());
  }

  @Test
  public void testFromHistogram() {
    final Set<String> fieldKeys = ImmutableSet.of(
      "count",
      "min",
      "max",
      "mean",
      "std-dev",
      "50-percentile",
      "75-percentile",
      "95-percentile",
      "99-percentile",
      "999-percentile",
      "run-count"
    );

    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    when(parser.parse("some.metric.name")).thenReturn(
      DropwizardMeasurement.create("Measurement", MEASUREMENT_TAGS, Optional.empty())
    );

    final Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir());
    histogram.update(15L);
    histogram.update(70L);
    histogram.update(100L);

    final InfluxDbMeasurement measurement = transformer.fromHistogram("some.metric.name", histogram, 90210L);
    assertEquals("should parse name from full metric key", "Measurement", measurement.name());
    assertEquals("should add global and measurement tags", ALL_TAGS, measurement.tags());
    assertEquals("should timestamp measurement", 90210L, measurement.timestamp());
    assertEquals("should add all histogram fields", fieldKeys, measurement.fields().keySet());
  }

  @Test
  public void testFromCounters_Ungrouped() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, false);

    final List<Counter> counters = ImmutableList.of(new Counter(), new Counter());
    counters.get(0).inc(15L);
    counters.get(1).inc(6L);

    final Map<String, Counter> cMap = ImmutableMap.of(
      "some.stuff.queued", counters.get(0),
      "some.stuff.processed", counters.get(1)
    );

    when(parser.parse("some.stuff.queued")).thenReturn(
      DropwizardMeasurement.create("some.stuff.queued", MEASUREMENT_TAGS, Optional.empty())
    );

    when(parser.parse("some.stuff.processed")).thenReturn(
      DropwizardMeasurement.create("some.stuff.processed", MEASUREMENT_TAGS, Optional.empty())
    );

    final List<InfluxDbMeasurement> expected = ImmutableList.of(
      InfluxDbMeasurement.create("some.stuff.queued", ALL_TAGS, ImmutableMap.of("count", "15i"), 90210L),
      InfluxDbMeasurement.create("some.stuff.processed", ALL_TAGS, ImmutableMap.of("count", "6i"), 90210L)
    );

    final List<InfluxDbMeasurement> measurements = transformer.fromCounters(cMap, 90210L);
    assertEquals("should not group counter measurements", expected, measurements);
  }

  @Test
  public void testFromCounters_Grouped() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    final List<Counter> counters = ImmutableList.of(new Counter(), new Counter());
    counters.get(0).inc(15L);
    counters.get(1).inc(6L);

    final Map<String, Counter> cMap = ImmutableMap.of(
      "some.stuff.queued", counters.get(0),
      "some.stuff.processed", counters.get(1)
    );

    when(parser.parse("some.stuff")).thenReturn(
      DropwizardMeasurement.create("some.stuff", MEASUREMENT_TAGS, Optional.empty())
    );

    final List<InfluxDbMeasurement> expected = ImmutableList.of(
      InfluxDbMeasurement.create("some.stuff", ALL_TAGS, ImmutableMap.of("queued", "15i", "processed", "6i"), 90210L)
    );

    final List<InfluxDbMeasurement> measurements = transformer.fromCounters(cMap, 90210L);
    assertEquals("should group counters by tags and prefix", expected, measurements);
  }

  @Test
  public void testFromGauges_Ungrouped() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, false);

    final Map<String, Gauge> gauges = ImmutableMap.of(
      "some.stuff.queued", () -> 12,
      "some.stuff.processed", () -> 15
    );

    when(parser.parse("some.stuff.queued")).thenReturn(
      DropwizardMeasurement.create("some.stuff.queued", MEASUREMENT_TAGS, Optional.empty())
    );

    when(parser.parse("some.stuff.processed")).thenReturn(
      DropwizardMeasurement.create("some.stuff.processed", MEASUREMENT_TAGS, Optional.empty())
    );

    final List<InfluxDbMeasurement> expected = ImmutableList.of(
      InfluxDbMeasurement.create("some.stuff.queued", ALL_TAGS, ImmutableMap.of("value", "12i"), 90210L),
      InfluxDbMeasurement.create("some.stuff.processed", ALL_TAGS, ImmutableMap.of("value", "15i"), 90210L)
    );

    final List<InfluxDbMeasurement> measurements = transformer.fromGauges(gauges, 90210L);
    assertEquals("should not group gauge measurements", expected, measurements);
  }

  @Test
  public void testFromGauges_Grouped() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    final Map<String, Gauge> gauges = ImmutableMap.of(
      "some.stuff.queued", () -> 12,
      "some.stuff.processed", () -> 15
    );

    when(parser.parse("some.stuff")).thenReturn(
      DropwizardMeasurement.create("some.stuff", MEASUREMENT_TAGS, Optional.empty())
    );

    final List<InfluxDbMeasurement> expected = ImmutableList.of(
      InfluxDbMeasurement.create("some.stuff", ALL_TAGS, ImmutableMap.of("queued", "12i", "processed", "15i"), 90210L)
    );

    final List<InfluxDbMeasurement> measurements = transformer.fromGauges(gauges, 90210L);
    assertEquals("should group gauges by tags and prefix", expected, measurements);
  }

  @Test
  public void testFromKeyValue() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, false);

    when(parser.parse("some.stuff.queued")).thenReturn(
      DropwizardMeasurement.create("some.stuff.queued", MEASUREMENT_TAGS, Optional.empty())
    );

    final InfluxDbMeasurement measurement = transformer.fromKeyValue("some.stuff.queued", "some_key", 12L, 90210L);

    assertEquals("should map values directly to measurement",
      InfluxDbMeasurement.create("some.stuff.queued", ALL_TAGS, ImmutableMap.of("some_key", "12i"), 90210L),
      measurement);
  }

  @Test
  public void testGroupValues_Inline() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    final Map<String, Gauge> gauges = ImmutableMap.of(
      "Measurement queued", () -> 12,
      "Measurement processed", () -> 15
    );

    when(parser.parse("Measurement queued")).thenReturn(
      DropwizardMeasurement.create("Measurement", MEASUREMENT_TAGS, Optional.of("queued"))
    );

    when(parser.parse("Measurement processed")).thenReturn(
      DropwizardMeasurement.create("Measurement", MEASUREMENT_TAGS, Optional.of("processed"))
    );

    final Map<GroupKey, Map<String, Object>> expected = ImmutableMap.of(
      GroupKey.create("Measurement", MEASUREMENT_TAGS), ImmutableMap.of("queued", 12, "processed", 15)
    );

    final Map<GroupKey, Map<String, Object>> groups = transformer.groupValues(gauges, "unused_default_key", Gauge::getValue);
    assertEquals("should group values with inlined keys", expected, groups);
  }

  @Test
  public void testGroupValues_CountingGauges() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    final Map<String, Gauge> gauges = ImmutableMap.of(
      "some.stuff.queued.count", () -> 12,
      "some.stuff.processed.count", () -> 15
    );

    when(parser.parse("some.stuff")).thenReturn(
      DropwizardMeasurement.create("some.stuff", MEASUREMENT_TAGS, Optional.empty())
    );

    final Map<GroupKey, Map<String, Object>> expected = ImmutableMap.of(
      GroupKey.create("some.stuff", MEASUREMENT_TAGS), ImmutableMap.of("queued.count", 12, "processed.count", 15)
    );

    final Map<GroupKey, Map<String, Object>> groups = transformer.groupValues(gauges, "unused_default_key", Gauge::getValue);
    assertEquals("should ignore .count postfix when parsing groups", expected, groups);
  }

  @Test
  public void testGroupValues_NoDotIndex() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    final Map<String, Gauge> gauges = ImmutableMap.of(
      "some_stuff_queued", () -> 12,
      "some_stuff_processed", () -> 15
    );

    when(parser.parse("some_stuff_queued")).thenReturn(
      DropwizardMeasurement.create("some.stuff.queued", MEASUREMENT_TAGS, Optional.empty())
    );

    when(parser.parse("some_stuff_processed")).thenReturn(
      DropwizardMeasurement.create("some.stuff.processed", MEASUREMENT_TAGS, Optional.empty())
    );

    final Map<GroupKey, Map<String, Object>> expected = ImmutableMap.of(
      GroupKey.create("some.stuff.queued", MEASUREMENT_TAGS), ImmutableMap.of("default_key", 12),
      GroupKey.create("some.stuff.processed", MEASUREMENT_TAGS), ImmutableMap.of("default_key", 15)
    );

    final Map<GroupKey, Map<String, Object>> groups = transformer.groupValues(gauges, "default_key", Gauge::getValue);
    assertEquals("should use fully parsed key and default value", expected, groups);
  }

  @Test
  public void testGroupValues_WithDotIndex() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    final Map<String, Gauge> gauges = ImmutableMap.of(
      "some.stuff.queued", () -> 12,
      "some.stuff.processed", () -> 15
    );

    when(parser.parse("some.stuff")).thenReturn(
      DropwizardMeasurement.create("some.stuff", MEASUREMENT_TAGS, Optional.empty())
    );

    final Map<GroupKey, Map<String, Object>> expected = ImmutableMap.of(
      GroupKey.create("some.stuff", MEASUREMENT_TAGS), ImmutableMap.of("queued", 12, "processed", 15)
    );

    final Map<GroupKey, Map<String, Object>> groups = transformer.groupValues(gauges, "unused_default_key", Gauge::getValue);
    assertEquals("should group values by field postfix", expected, groups);
  }

  @Test
  public void testGroupValues_WithSeparateTags() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    final Map<String, Gauge> gauges = ImmutableMap.of(
      "some.jumping.stuff.queued", () -> 12,
      "some.leaning.stuff.processed", () -> 15
    );

    final ImmutableMap<String, String> jumpingTag = ImmutableMap.of("action", "jumping");
    final ImmutableMap<String, String> leaningTag = ImmutableMap.of("action", "leaning");

    when(parser.parse("some.jumping.stuff")).thenReturn(
      DropwizardMeasurement.create("some.stuff", jumpingTag, Optional.empty())
    );

    when(parser.parse("some.leaning.stuff")).thenReturn(
      DropwizardMeasurement.create("some.stuff", leaningTag, Optional.empty())
    );

    final Map<GroupKey, Map<String, Object>> expected = ImmutableMap.of(
      GroupKey.create("some.stuff", jumpingTag), ImmutableMap.of("queued", 12),
      GroupKey.create("some.stuff", leaningTag), ImmutableMap.of("processed", 15)
    );

    final Map<GroupKey, Map<String, Object>> groups = transformer.groupValues(gauges, "unused_default_key", Gauge::getValue);
    assertEquals("should separate measurements by tag", expected, groups);
  }

  @Test
  public void testFromValueGroup() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);

    final Map<String, Object> objFields = ImmutableMap.of(
      "some", true,
      "fields", 5
    );

    final Map<String, String> strFields = ImmutableMap.of(
      "some", "true",
      "fields", "5i"
    );

    assertEquals("should convert value group to influx measurement with global tags and stringy fields",
      Optional.of(InfluxDbMeasurement.create("Measurement", ALL_TAGS, strFields, 90210L)),
      transformer.fromValueGroup(GroupKey.create("Measurement", MEASUREMENT_TAGS), objFields, 90210L));
  }

  @Test
  public void testFromValueGroup_InvalidField() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);
    final GroupKey key = GroupKey.create("Measurement", MEASUREMENT_TAGS);

    final Map<String, Object> objFields = ImmutableMap.of(
      "some", true,
      "bad", Arrays.asList(1, 2, 3),
      "fields", 5
    );

    final Map<String, String> strFields = ImmutableMap.of(
      "some", "true",
      "fields", "5i"
    );

    assertEquals("should silently drop invalid values",
      Optional.of(InfluxDbMeasurement.create("Measurement", ALL_TAGS, strFields, 90210L)),
      transformer.fromValueGroup(key, objFields, 90210L));
  }

  @Test
  public void testFromValueGroup_InvalidMeasurement() {
    final DropwizardMeasurementParser parser = mock(DropwizardMeasurementParser.class);
    final DropwizardTransformer transformer = transformerWithParser(parser, true);
    final GroupKey key = GroupKey.create("Measurement", MEASUREMENT_TAGS);

    final Map<String, Object> objFields = ImmutableMap.of(
      "all", Float.NaN,
      "bad", Arrays.asList(1, 2, 3),
      "fields", Float.NaN
    );

    assertEquals("should silently drop invalid measurements",
      Optional.empty(),
      transformer.fromValueGroup(key, objFields, 90210L));
  }

  // ===================================================================================================================
  // Test helpers

  private static final ImmutableMap<String, String> BASE_TAGS = ImmutableMap.of(
    "some", "simple",
    "global", "tags"
  );

  private static final ImmutableMap<String, String> MEASUREMENT_TAGS = ImmutableMap.of(
    "more", "specific",
    "measurement", "tags"
  );

  private static final ImmutableMap<String, String> ALL_TAGS = new ImmutableMap.Builder<String, String>()
    .putAll(BASE_TAGS)
    .putAll(MEASUREMENT_TAGS)
    .build();

  private static DropwizardTransformer transformerWithParser(final DropwizardMeasurementParser parser, final boolean group) {
    return new DropwizardTransformer(
      BASE_TAGS,
      parser,
      group,
      group,
      // Dropwizard Defaults
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS
    );
  }

  private static DropwizardTransformer transformerWithUnits(final TimeUnit rateUnits, final TimeUnit durationUnits) {
    return new DropwizardTransformer(
      ImmutableMap.of(),
      DropwizardMeasurementParser.withTemplates(ImmutableMap.of()),
      true,
      true,
      rateUnits,
      durationUnits
    );
  }
}
