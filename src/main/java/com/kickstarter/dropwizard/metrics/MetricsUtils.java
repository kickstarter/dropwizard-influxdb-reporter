package com.kickstarter.dropwizard.metrics;

import com.kickstarter.dropwizard.metrics.influxdb.transformer.DropwizardMeasurement;

import java.util.Map;
import java.util.Optional;

public final class MetricsUtils {
  private MetricsUtils(){}

  /**
   * Generate the name for an InfluxDb metric with the given measurement name and tags.
   *
   * <pre>{@code
   *   final Timer timer = registry.timer(influxName("Measurement", ImmutableMap.of("action", "restore")));
   * }</pre>
   */
  public static String influxName(final String measurement, final Map<String, String> tags) {
    return DropwizardMeasurement.create(measurement, tags, Optional.empty()).toString();
  }

  /**
   * Generate the name for an InfluxDb metric with the given measurement name, field name, and tags.
   *
   * <pre>{@code
   * registry.gauge(
   *     influxName("Measurement", "results", ImmutableMap.of("method", "retrieveCats")),
   *     results::size
   * );
   * }</pre>
   */
  public static String influxName(final String measurement, final String field, final Map<String, String> tags) {
    return DropwizardMeasurement.create(measurement, tags, Optional.of(field)).toString();
  }
}
