package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.auto.value.AutoValue;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toMap;

/**
 * A Dropwizard measurement associated with a {@link MetricRegistry} component
 * (e.g. {@link Timer timers}, {@link Counter counters}, and {@link Gauge gauges}).
 *
 * <p>Serialized in the following format:
 * <p>{@code
 *   measurement_name[,tag1=val1,tag2=val2][ field_name]
 * }
 */
@AutoValue
public abstract class DropwizardMeasurement {
  // Serializer constants.
  private static final String FIELD_SEPARATOR = " ";
  private static final String TAG_SEPARATOR = ",";
  private static final String VALUE_SEPARATOR = "=";
  private static final String ESCAPE_CHARS = " |,|=";
  private static final String SANITIZER = "-";

  // Fields
  abstract String name();
  abstract Map<String, String> tags();

  // Provided field name for gauges; used for gauge grouping.
  // This field is only set for non-templated measurements;
  // templates will be parsed for field names.
  abstract Optional<String> field();

  /**
   * Sanitizes InfluxDb escape characters.
   */
  private static String sanitize(final String s) {
    return s.replaceAll(ESCAPE_CHARS, SANITIZER);
  }

  /**
   * Creates a new {@link DropwizardMeasurement} with sanitized values.
   */
  public static DropwizardMeasurement create(final String name, final Map<String, String> tags, final Optional<String> field) {
    final Map<String, String> sanitizedTags = tags.entrySet()
      .stream()
      .collect(toMap(
        e -> DropwizardMeasurement.sanitize(e.getKey()),
        e -> DropwizardMeasurement.sanitize(e.getValue())
      ));

    return new AutoValue_DropwizardMeasurement(sanitize(name), sanitizedTags, field.map(DropwizardMeasurement::sanitize));
  }

  /**
   * Deserializes a measurement line string in the form:
   * <p>{@code
   *   measurement_name[,tag1=val1,tag2=val2][ field_name]
   * }
   */
  public static DropwizardMeasurement fromLine(final String line) {
    // split measurement-name/tags and field name
    final String[] initialParts = line.split(FIELD_SEPARATOR);
    if (initialParts.length > 2) {
      throw new IllegalArgumentException("too many spaces in measurement line");
    }

    // parse optional field
    final Optional<String> field;
    if (initialParts.length == 2) {
      field = Optional.of(initialParts[1]);
    } else {
      field = Optional.empty();
    }

    // split measurement-name and tags
    final String[] parts = initialParts[0].split(TAG_SEPARATOR);
    final String name = parts[0];

    // parse tags as a key-val map
    final Map<String, String> tags = Arrays.asList(parts)
      .subList(1, parts.length)
      .stream()
      .map(s -> s.split(VALUE_SEPARATOR))
      .filter(arr -> arr.length == 2)
      .collect(toMap(arr -> arr[0], arr -> arr[1]));

    // if the sizes aren't one-to-one, we filtered some tag parts out.
    if (tags.size() != parts.length - 1) {
      throw new IllegalArgumentException("tags must contain exactly one '=' character");
    }

    return DropwizardMeasurement.create(name, tags, field);
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(name());

    tags().forEach((k, v) -> {
      builder.append(TAG_SEPARATOR);
      builder.append(k);
      builder.append(VALUE_SEPARATOR);
      builder.append(v);
    });

    if (field().isPresent()) {
      builder.append(FIELD_SEPARATOR);
      builder.append(field().orElse(""));
    }

    return builder.toString();
  }
}
