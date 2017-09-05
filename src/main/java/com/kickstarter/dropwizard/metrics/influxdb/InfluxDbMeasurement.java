package com.kickstarter.dropwizard.metrics.influxdb;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;

/**
 * An InfluxDB measurement representation.
 */
@AutoValue
public abstract class InfluxDbMeasurement {
  private static final List<Class> VALID_FIELD_CLASSES = ImmutableList.of(
    Number.class, Character.class, String.class, Boolean.class
  );

  public abstract String name();
  public abstract Map<String, String> tags();
  public abstract Map<String, String> fields();
  public abstract long timestamp();

  /**
   * Creates a new {@link InfluxDbMeasurement}.
   *
   * Assumes that these fields have already been escaped;
   * by default, any invalid measurement is silently dropped by InfluxDB.
    */
  public static InfluxDbMeasurement create(final String name,
                                           final Map<String, String> tags,
                                           final Map<String, String> fields,
                                           final long timestamp) {
    Preconditions.checkArgument(!name.isEmpty(), "InfluxDbMeasurement must contain a non-empty name");
    Preconditions.checkArgument(!fields.isEmpty(), "InfluxDbMeasurement must contain at least one field");
    return new AutoValue_InfluxDbMeasurement(name, tags, fields, timestamp);
  }

  /**
   * Returns the measurement in InfluxDB line notation with the provided timestamp precision.
   *
   * <p>Does not support sub-millisecond timestamp precision.
   */
  public String toLine() {
    final StringBuilder sb = new StringBuilder();
    sb.append(name());

    if (!tags().isEmpty()) {
      sb.append(',');
      sb.append(joinPairs(tags()));
    }

    if (!fields().isEmpty()) {
      sb.append(' ');
      sb.append(joinPairs(fields()));
    }

    sb.append(' ');
    sb.append(TimeUnit.NANOSECONDS.convert(timestamp(), TimeUnit.MILLISECONDS));
    return sb.toString();
  }

  @Override
  public String toString() {
    return toLine();
  }

  /**
   * Returns a comma-separated key-value formatting of {@code pairs}.
   *
   * <p>e.g. "k1=v1,k2=v2,k3=v3"
   */
  @VisibleForTesting static String joinPairs(final Map<String, String> pairs) {
    final List<String> pairStrs = pairs.entrySet()
      .parallelStream()
      .map(e -> String.join("=", e.getKey(), e.getValue()))
      .collect(toList());

    return String.join(",", pairStrs);
  }

  // ===================================================================================================================
  // Builder

  /**
   * A builder for {@link InfluxDbMeasurement}.
   */
  public static class Builder {
    private final String name;
    private final long timestamp;
    private final Map<String, String> tags = new HashMap<>();
    private final Map<String, String> fields = new HashMap<>();

    public Builder(final String name, final long timestamp) {
      this.name = name;
      this.timestamp = timestamp;
    }

    public boolean isValid() {
      return !name.isEmpty() && !fields.isEmpty();
    }

    /**
     * Adds all key-value pairs to the tags map.
     */
    public Builder putTags(final Map<String, String> items) {
      tags.putAll(items);
      return this;
    }

    /**
     * Adds the key-value pair to the tags map.
     */
    public Builder putTag(final String key, final String value) {
      tags.put(key, value);
      return this;
    }

    /**
     * Adds all key-value pairs to the fields map.
     *
     * @throws IllegalArgumentException if any value is not a String or primitive.
     */
    public <T> Builder putFields(final Map<String, T> fields) {
      fields.forEach(this::putField);
      return this;
    }

    /**
     * Adds all key-value pairs to the fields map.
     */
    public <T> Builder tryPutFields(final Map<String, T> fields,
                                    final Consumer<IllegalArgumentException> exceptionHandler) {
      for (final Map.Entry<String, T> field : fields.entrySet()) {
        try {
          putField(field.getKey(), field.getValue());
        } catch (IllegalArgumentException e) {
          exceptionHandler.accept(e);
        }
      }
      return this;
    }

    /**
     * Adds the given key-value pair to the fields map.
     *
     * @throws IllegalArgumentException if any value is not a String, primitive,
     *         or Collection of Strings and primitives.
     */
    public <T> Builder putField(final String key, final T value) {
      if (value instanceof Collection<?>) {
        final Collection collection = (Collection) value;
        final String collString = validatedPrimitiveCollection(key, collection);
        fields.put(key, collString);
      } else if (value != null) {
        final Optional<String> fieldString = validatedPrimitiveField(key, value);
        fieldString.ifPresent(s -> fields.put(key, s));
      }

      return this;
    }

    /**
     * Validates that the collection only contains Strings and primitives.
     *
     * @return the collection as a string, if valid.
     * @throws IllegalArgumentException if any collection value is not a string or primitive.
     */
    private static String validatedPrimitiveCollection(final String key, final Collection collection) {
      for (final Object value : collection) {
        if (!isValidField(value)) {
          throw new IllegalArgumentException(
            String.format(
              "InfluxDbMeasurement collection field '%s' must contain only Strings and primitives: invalid field '%s'",
              key, value
            )
          );
        }
      }

      return collection.toString();
    }

    /**
     * Validates that the field is a String or primitive.
     *
     * @return true if the field is a String or primitive.
     */
    private static <T> boolean isValidField(final T value) {
      return VALID_FIELD_CLASSES.stream().anyMatch(klass -> klass.isInstance(value));
    }

    /**
     * Validates that the field is a String or primitive.
     *
     * @return an Optional containing the stringified field, or Optional.empty()
     *         if the field is an invalid primitive field.
     *
     * @throws IllegalArgumentException if the field is not a string or primitive.
     */
    private static <T> Optional<String> validatedPrimitiveField(final String key, final T value) {
      if (value instanceof Float) {
        final float f = (Float) value;
        if (!Float.isNaN(f) && !Float.isInfinite(f)) {
          return Optional.of(String.valueOf(f));
        }
      } else if (value instanceof Double) {
        final double d = (Double) value;
        if (!Double.isNaN(d) && !Double.isInfinite(d)) {
          return Optional.of(String.valueOf(d));
        }
      } else if (value instanceof Number) {
        // Serialize Byte, Short, Integer, and Long values as integers.
        return Optional.of(String.format("%di", ((Number) value).longValue()));
      } else if (value instanceof String || value instanceof Character || value instanceof Boolean) {
        return Optional.of(value.toString());
      } else {
        throw new IllegalArgumentException(
          String.format(
            "InfluxDbMeasurement field '%s' must be a String, primitive, or Collection: invalid field '%s'",
            key, value
          )
        );
      }

      return Optional.empty();
    }

    public InfluxDbMeasurement build() {
      return InfluxDbMeasurement.create(name, tags, fields, timestamp);
    }
  }
}
