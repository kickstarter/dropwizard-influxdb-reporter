package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.kickstarter.dropwizard.metrics.influxdb.InfluxDbMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * A transformer from Dropwizard metric objects to tagged and grouped {@link InfluxDbMeasurement}s.
 * 
 * <p>Supports global tags, tagged templating, counter/gauge grouping, and per-metric tagging.
 */
public class DropwizardTransformer {
  private static final Logger log = LoggerFactory.getLogger(DropwizardTransformer.class);

  private final Map<String, String> baseTags;
  private final DropwizardMeasurementParser parser;
  private final boolean groupCounters;
  private final boolean groupGauges;

  private final long rateFactor;
  private final long durationFactor;

  public DropwizardTransformer(final Map<String, String> baseTags,
                               final DropwizardMeasurementParser parser,
                               final boolean groupCounters,
                               final boolean groupGauges,
                               final TimeUnit rateUnit,
                               final TimeUnit durationUnit) {
    this.baseTags = baseTags;
    this.parser = parser;
    this.groupCounters = groupCounters;
    this.groupGauges = groupGauges;
    this.rateFactor = rateUnit.toSeconds(1);
    this.durationFactor = durationUnit.toNanos(1);
  }

  @VisibleForTesting double convertDuration(final double duration) {
    return duration / durationFactor;
  }

  @VisibleForTesting double convertRate(final double rate) {
    return rate * rateFactor;
  }

  // ===================================================================================================================
  // timers

  /**
   * Build a List of {@link InfluxDbMeasurement}s from a timer map.
   */
  public List<InfluxDbMeasurement> fromTimers(final Map<String, Timer> timers, final long timestamp) {
    return timers.entrySet().stream()
      .map(e -> fromTimer(e.getKey(), e.getValue(), timestamp))
      .collect(toList());
  }

  /**
   * Build an {@link InfluxDbMeasurement} from a timer.
   */
  @VisibleForTesting InfluxDbMeasurement fromTimer(final String metricName, final Timer t, final long timestamp) {
    final Snapshot snapshot = t.getSnapshot();
    final DropwizardMeasurement measurement = parser.parse(metricName);

    final Map<String, String> tags = new HashMap<>(baseTags);
    tags.putAll(measurement.tags());

    return new InfluxDbMeasurement.Builder(measurement.name(), timestamp)
      .putTags(tags)
      .putField("count", snapshot.size())
      .putField("min", convertDuration(snapshot.getMin()))
      .putField("max", convertDuration(snapshot.getMax()))
      .putField("mean", convertDuration(snapshot.getMean()))
      .putField("std-dev", convertDuration(snapshot.getStdDev()))
      .putField("50-percentile", convertDuration(snapshot.getMedian()))
      .putField("75-percentile", convertDuration(snapshot.get75thPercentile()))
      .putField("95-percentile", convertDuration(snapshot.get95thPercentile()))
      .putField("99-percentile", convertDuration(snapshot.get99thPercentile()))
      .putField("999-percentile", convertDuration(snapshot.get999thPercentile()))
      .putField("one-minute", convertRate(t.getOneMinuteRate()))
      .putField("five-minute", convertRate(t.getFiveMinuteRate()))
      .putField("fifteen-minute", convertRate(t.getFifteenMinuteRate()))
      .putField("mean-minute", convertRate(t.getMeanRate()))
      .putField("run-count", t.getCount())
      .build();
  }

  // ===================================================================================================================
  // meters

  /**
   * Build a List of {@link InfluxDbMeasurement}s from a meter map.
   */
  public List<InfluxDbMeasurement> fromMeters(final Map<String, Meter> meters, final long timestamp) {
    return meters.entrySet().stream()
      .map(e -> fromMeter(e.getKey(), e.getValue(), timestamp))
      .collect(toList());
  }

  /**
   * Build an {@link InfluxDbMeasurement} from a meter.
   */
  @VisibleForTesting InfluxDbMeasurement fromMeter(final String metricName, final Meter mt, final long timestamp) {
    final DropwizardMeasurement measurement = parser.parse(metricName);

    final Map<String, String> tags = new HashMap<>(baseTags);
    tags.putAll(measurement.tags());

    return new InfluxDbMeasurement.Builder(measurement.name(), timestamp)
      .putTags(tags)
      .putField("count", mt.getCount())
      .putField("one-minute", convertRate(mt.getOneMinuteRate()))
      .putField("five-minute", convertRate(mt.getFiveMinuteRate()))
      .putField("fifteen-minute", convertRate(mt.getFifteenMinuteRate()))
      .putField("mean-minute", convertRate(mt.getMeanRate()))
      .build();
  }

  // ===================================================================================================================
  // histograms

  /**
   * Build a List of {@link InfluxDbMeasurement}s from a histogram map.
   */
  public List<InfluxDbMeasurement> fromHistograms(final Map<String, Histogram> histograms, final long timestamp) {
    return histograms.entrySet().stream()
      .map(e -> fromHistogram(e.getKey(), e.getValue(), timestamp))
      .collect(toList());
  }

  /**
   * Build an {@link InfluxDbMeasurement} from a histogram.
   */
  @VisibleForTesting InfluxDbMeasurement fromHistogram(final String metricName, final Histogram h, final long timestamp) {
    final Snapshot snapshot = h.getSnapshot();
    final DropwizardMeasurement measurement = parser.parse(metricName);

    final Map<String, String> tags = new HashMap<>(baseTags);
    tags.putAll(measurement.tags());

    return new InfluxDbMeasurement.Builder(measurement.name(), timestamp)
      .putTags(tags)
      .putField("count", snapshot.size())
      .putField("min", snapshot.getMin())
      .putField("max", snapshot.getMax())
      .putField("mean", snapshot.getMean())
      .putField("std-dev", snapshot.getStdDev())
      .putField("50-percentile", snapshot.getMedian())
      .putField("75-percentile", snapshot.get75thPercentile())
      .putField("95-percentile", snapshot.get95thPercentile())
      .putField("99-percentile", snapshot.get99thPercentile())
      .putField("999-percentile", snapshot.get999thPercentile())
      .putField("run-count", h.getCount())
      .build();
  }

  // ===================================================================================================================
  // groupable measurements: counters and gauges

  /**
   * Build a List of {@link InfluxDbMeasurement}s from a counter map.
   */
  public List<InfluxDbMeasurement> fromCounters(final Map<String, Counter> counters, final long timestamp) {
    return fromCounterOrGauge(counters, "count", Counter::getCount, timestamp, groupCounters);
  }

  /**
   * Build a List of {@link InfluxDbMeasurement}s from a gauge map.
   */
  public List<InfluxDbMeasurement> fromGauges(final Map<String, Gauge> gauges, final long timestamp) {
    return fromCounterOrGauge(gauges, "value", Gauge::getValue, timestamp, groupGauges);
  }

  private <T, R> List<InfluxDbMeasurement> fromCounterOrGauge(final Map<String, T> items,
                                                              final String defaultFieldName,
                                                              final Function<T, R> valueExtractor,
                                                              final long timestamp,
                                                              final boolean group) {
    if (group) {
      final Map<GroupKey, Map<String, R>> groupedItems = groupValues(items, defaultFieldName, valueExtractor);
      return groupedItems.entrySet().stream()
        .map(entry -> fromValueGroup(entry.getKey(), entry.getValue(), timestamp))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toList());
    } else {
      return items.entrySet().stream()
        .map(entry -> fromKeyValue(entry.getKey(), defaultFieldName, valueExtractor.apply(entry.getValue()), timestamp))
        .collect(toList());
    }
  }

  /**
   * Build an {@link InfluxDbMeasurement} directly from given values.
   */
  @VisibleForTesting <T> InfluxDbMeasurement fromKeyValue(final String metricName,
                                                          final String defaultFieldName,
                                                          final T value,
                                                          final long timestamp) {
    final DropwizardMeasurement measurement = parser.parse(metricName);
    final Map<String, String> tags = new HashMap<>(baseTags);
    tags.putAll(measurement.tags());

    return new InfluxDbMeasurement.Builder(measurement.name(), timestamp)
      .putTags(tags)
      .putField(defaultFieldName, value)
      .build();
  }

  /**
   * Groups {@code items} into a set of measurements marked by the measurement name and tag set.
   *
   * <p>This method separates measurement names and fields by the following logic:

   * <p>For {@link DropwizardMeasurement}-style line notation, we parse measurements and fields
   * directly via {@link DropwizardMeasurement#fromLine(String)}.
   *
   *
   * <p>For Dropwizard-style dot notation, we read the last part as a field name. If there is
   * only one part, we use the provided default field name.
   * <p>Metrics that end with `.count` will be parsed with the last two parts as a field name.
   *
   * <p>e.g.
   * <table>
   *   <tr>
   *     <td><b>Full Key</b></td>
   *     <td><b>Parsed Group</b></td>
   *     <td><b>Parsed Field</b></td>
   *   </tr>
   *   <tr>
   *     <td>Measurement,action=restore someField</td>
   *     <td>Measurement,action=restore</td>
   *     <td>someField</td>
   *   </tr>
   *   <tr>
   *     <td>Measurement,action=restore</td>
   *     <td>Measurement,action=restore</td>
   *     <td>defaultFieldName</td>
   *   </tr>
   *   <tr>
   *     <td>jvm</td>
   *     <td>jvm</td>
   *     <td>defaultFieldName</td>
   *   </tr>
   *   <tr>
   *     <td>jvm.threads.deadlock</td>
   *     <td>jvm.threads</td>
   *     <td>deadlock</td>
   *   </tr>
   *   <tr>
   *     <td>jvm.threads.deadlock.count</td>
   *     <td>jvm.threads</td>
   *     <td>deadlock.count</td>
   *   </tr>
   * </table>
   *
   * @return A map from {@link GroupKey GroupKeys} to the group's field map.
   */
  @VisibleForTesting <T, R> Map<GroupKey, Map<String, R>> groupValues(final Map<String, T> items,
                                                                      final String defaultFieldName,
                                                                      final Function<T, R> valueExtractor) {
    final Map<GroupKey, Map<String, R>> groupedValues = new HashMap<>();

    items.forEach((key, item) -> {
      final String measurementKey;
      final String field;

      if (key.contains(" ") || key.contains(",")) {
        // Inlined key with tag or field -- formatted as seen in Measurement.toString().
        measurementKey = key;
        field = parser.parse(key).field().orElse(defaultFieldName);
      } else {
        // Templated key -- formatted in Dropwizard/Graphite-style directory notation.

        // If the key ends in `.count`, we try to include the previous part in the field name.
        // This is an odd hack to group Dropwizard jvm.threads gauge measurements.
        // e.g. for 'jvm.threads.deadlock.count': key=jvm.threads, field=deadlock.count.
        final boolean hasCountPostfix = key.endsWith(".count");
        final String mainKey = hasCountPostfix ? key.substring(0, key.length() - 6) : key;

        // Parse the field name from the last key part.
        // e.g. for `jvm.memory.heap`: key=jvm.memory, field=heap.
        final int lastDotIndex = mainKey.lastIndexOf(".");
        if (lastDotIndex == -1) {
          // only one key part; use the default field name.
          measurementKey = mainKey;
          field = hasCountPostfix ? "count" : defaultFieldName;
        } else {
          // parse the last part as a field name.
          measurementKey = mainKey.substring(0, lastDotIndex);
          // use `key` instead of `mainKey` here
          // to include `.count` in the field name.
          field = key.substring(lastDotIndex + 1);
        }
      }

      final DropwizardMeasurement measurement = parser.parse(measurementKey);
      final GroupKey groupKey = GroupKey.create(measurement.name(), measurement.tags());

      final Map<String, R> fields = groupedValues.getOrDefault(groupKey, new HashMap<>());
      fields.put(field, valueExtractor.apply(item));
      groupedValues.put(groupKey, fields);
    });

    return groupedValues;
  }

  /**
   * Build an {@link InfluxDbMeasurement} from a group key and field map.
   */
  @VisibleForTesting <T> Optional<InfluxDbMeasurement> fromValueGroup(final GroupKey groupKey,
                                                                      final Map<String, T> fields,
                                                                      final long timestamp) {
    final Map<String, String> tags = new HashMap<>(baseTags);
    tags.putAll(groupKey.tags());

    final InfluxDbMeasurement.Builder builder =
      new InfluxDbMeasurement.Builder(groupKey.measurement(), timestamp)
        .putTags(tags)
        .tryPutFields(fields, e -> log.warn(e.getMessage()));

    if (!builder.isValid()) {
      log.warn("Measurement has no valid fields: {}", groupKey.measurement());
      return Optional.empty();
    }

    return Optional.of(builder.build());
  }
}
