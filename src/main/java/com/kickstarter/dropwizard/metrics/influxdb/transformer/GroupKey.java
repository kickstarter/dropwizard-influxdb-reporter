package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;

import java.util.Map;

@AutoValue
abstract class GroupKey {
  abstract String measurement();
  abstract Map<String, String> tags();

  @VisibleForTesting static GroupKey create(final String measurement, final Map<String, String> tags) {
    return new AutoValue_GroupKey(measurement, tags);
  }
}
