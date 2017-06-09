package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class DropwizardMeasurementParserTest {
  private static final DropwizardMeasurementParser transformer = new DropwizardMeasurementParser(
    ImmutableMap.of("custom_metric", new TaggedPattern("\\.holy\\.(?<adjective>[A-Za-z]+)\\.cow\\.", "adjective"))
  );

  @Test
  public void testMeasurement_FromTemplate() {
    final DropwizardMeasurement measurement = transformer.parse(".holy.jumping.cow.");
    assertEquals(
      DropwizardMeasurement.create("custom_metric", ImmutableMap.of("adjective", "jumping"), Optional.empty()),
      measurement
    );
  }

  @Test
  public void testMeasurement_FromMeasurementLine() {
    final DropwizardMeasurement expected = DropwizardMeasurement.create(
      "custom_metric",
      ImmutableMap.of("adjective", "jumping", "more", "stuff"),
      Optional.of("im-a-field")
    );

    final DropwizardMeasurement measurement = transformer.parse(
      "custom_metric,adjective=jumping,more=stuff im-a-field"
    );

    assertEquals(expected, measurement);
  }
}
