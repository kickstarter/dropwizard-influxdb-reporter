package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A dropwizard measurement parser that holds metric templates for mapping between
 * directory-style dot notation and influx-style tags. If no template matches a
 * given metric name, it will attempt to parse it as a {@link DropwizardMeasurement} line.
 *
 * This transformer caches mappings from metric names to {@link DropwizardMeasurement}
 * objects to avoid extraneous regex matching and string parsing.
 */
public class DropwizardMeasurementParser {
  /**
   * Templates for provided Dropwizard metrics. I'm so sorry for these regexes.
   */
  private static final ImmutableMap<String, TaggedPattern> DROPWIZARD_METRIC_MAPPINGS = ImmutableMap.<String, TaggedPattern>builder()
    .put("health",          new TaggedPattern(".*\\.health\\.(?<check>.*)"))
    .put("resources",       new TaggedPattern(".*\\.resources?\\.(?<resource>[A-Za-z]+)\\.(?<method>[A-Za-z]+)", "resource", "method"))
    .put("jvm",             new TaggedPattern("^jvm$"))
    .put("jvm_attribute",   new TaggedPattern("jvm\\.attribute.*?"))
    .put("jvm_buffers",     new TaggedPattern("jvm\\.buffers\\.?(?<type>.*)", "type"))
    .put("jvm_classloader", new TaggedPattern("jvm\\.classloader.*"))
    .put("jvm_gc",          new TaggedPattern("jvm\\.gc\\.?(?<metric>.*)", "metric"))
    .put("jvm_memory",      new TaggedPattern("jvm\\.memory\\.?(?<metric>.*)", "metric"))
    .put("jvm_threads",     new TaggedPattern("jvm\\.threads\\.?(?<metric>.*)", "metric"))
    .put("logging",         new TaggedPattern("ch\\.qos\\.logback\\.core\\.Appender\\.(?<level>.*)", "level"))
    .put("raw_sql",         new TaggedPattern("org\\.skife\\.jdbi\\.v2\\.DBI\\.raw-sql"))
    .put("clients",         new TaggedPattern("org\\.apache\\.http\\.client\\.HttpClient\\.(?<client>.*)\\.(?<metric>.*)$", "client", "metric"))
    .put("client_connections", new TaggedPattern("org\\.apache\\.http\\.conn\\.HttpClientConnectionManager\\.(?<client>.*)", "client"))
    .put("connections",     new TaggedPattern("org\\.eclipse\\.jetty\\.server\\.HttpConnectionFactory\\.(?<port>[0-9]+).*", "port"))
    .put("thread_pools",    new TaggedPattern("org\\.eclipse\\.jetty\\.util\\.thread\\.QueuedThreadPool\\.(?<pool>.*)", "pool"))
    .put("http_server",     new TaggedPattern("io\\.dropwizard\\.jetty\\.MutableServletContextHandler\\.?(?<metric>.*)", "metric"))
    .put("data_sources",    new TaggedPattern("io\\.dropwizard\\.db\\.ManagedPooledDataSource\\.(?<metric>.*)", "metric"))
    .build();

  private final ImmutableMap<String, TaggedPattern> metricTemplates;
  private final Map<String, DropwizardMeasurement> cache = new HashMap<>();

  @VisibleForTesting DropwizardMeasurementParser(final ImmutableMap<String, TaggedPattern> metricTemplates) {
    this.metricTemplates = metricTemplates;
  }

  /**
   * Returns a new {@link DropwizardMeasurementParser} with default {@link #DROPWIZARD_METRIC_MAPPINGS}
   * and the user-provided {@code metricTemplates}.
   */
  public static DropwizardMeasurementParser withTemplates(final Map<String, TaggedPattern> metricTemplates) {
    return new DropwizardMeasurementParser(
      new ImmutableMap.Builder<String, TaggedPattern>()
        .putAll(metricTemplates)
        .putAll(DROPWIZARD_METRIC_MAPPINGS)
        .build()
    );
  }

  /**
   * Returns a {@link DropwizardMeasurement} from a matched template, or parses it as a measurement line.
   *
   * @throws IllegalArgumentException if the measurement does not match a template and is not in the
   * measurement line format.
   *
   * @see DropwizardMeasurement#fromLine(String)
   */
  /*package*/ DropwizardMeasurement parse(final String metricName) {
    return cache.computeIfAbsent(metricName, __ ->
      templatedMeasurement(metricName)
        .orElseGet(() -> DropwizardMeasurement.fromLine(metricName))
    );
  }

  /**
   * Searches the configured templates for a match to {@code metricName}.
   *
   * @return an Optional-wrapped {@link DropwizardMeasurement} with any matched tags, and no field value.
   */
  private Optional<DropwizardMeasurement> templatedMeasurement(final String metricName) {
    for (final Map.Entry<String, TaggedPattern> entry : metricTemplates.entrySet()) {
      final Optional<DropwizardMeasurement> measurement = entry.getValue().tags(metricName)
        .map(tags -> DropwizardMeasurement.create(entry.getKey(), tags, Optional.empty()));

      if (measurement.isPresent()) {
        return measurement;
      }
    }

    return Optional.empty();
  }
}
