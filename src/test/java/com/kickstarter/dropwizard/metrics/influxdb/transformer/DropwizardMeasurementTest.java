package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DropwizardMeasurementTest {
  @Test
  public void testCreate_EscapesValues() {
    final DropwizardMeasurement expected = DropwizardMeasurement.create(
      "Mode-lS-e-rv-e-r",
      ImmutableMap.of("k-e-y-", "-v-a-l-u-e-"),
      Optional.of("f-i-e--ld")
    );

    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Mode,lS,e=rv e r",
      ImmutableMap.of("k,e=y ", " v=a l,u=e "),
      Optional.of("f i=e,,ld")
    );

    assertEquals(expected, measurement);
  }

  @Test
  public void testFromLine_Deserializes() {
    final DropwizardMeasurement expected = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of(),
      Optional.empty()
    );
    assertEquals(expected, DropwizardMeasurement.fromLine("Measurement"));
  }

  @Test
  public void testFromLine_DeserializesWithTags() {
    final DropwizardMeasurement expected = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of("action", "restore", "model", "cf-2-005"),
      Optional.empty()
    );
    assertEquals(expected, DropwizardMeasurement.fromLine("Measurement,action=restore,model=cf-2-005"));
  }

  @Test
  public void testFromLine_DeserializesWithField() {
    final DropwizardMeasurement expected = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of(),
      Optional.of("i-am-a-field")
    );

    assertEquals(expected, DropwizardMeasurement.fromLine("Measurement i-am-a-field"));
  }

  @Test
  public void testFromLine_DeserializesWithTagsAndField() {
    final DropwizardMeasurement expected = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of("action", "restore", "model", "cf-2-005"),
      Optional.of("i-am-a-field")
    );

    assertEquals(expected, DropwizardMeasurement.fromLine("Measurement,action=restore,model=cf-2-005 i-am-a-field"));
  }

  @Test
  public void testFromLine_TooManyFieldSeparators() {
    try {
      DropwizardMeasurement.fromLine("Measurement,action=restore,model=cf-2-005 one_field too_many_fields");
      fail("Expected an exception to be thrown");
    } catch (final Exception thrown) {
      assertEquals(
        String.format("Unexpected exception: %s", thrown.getClass()),
        IllegalArgumentException.class,
        thrown.getClass()
      );

      assertEquals("too many spaces in measurement line", thrown.getMessage());
    }
  }

  @Test
  public void testFromLine_TooManyValueSeparators() {
    try {
      DropwizardMeasurement.fromLine("Measurement,action=restore=why,model=cf-2-005");
      fail("Expected an exception to be thrown");
    } catch (final Exception thrown) {
      assertEquals(
        String.format("Unexpected exception: %s", thrown.getClass()),
        IllegalArgumentException.class,
        thrown.getClass()
      );

      assertEquals("tags must contain exactly one '=' character", thrown.getMessage());
    }
  }

  @Test
  public void testFromLine_MissingValueSeparators() {
    try {
      DropwizardMeasurement.fromLine("Measurement,actionrestore,model=cf-2-005");
      fail("Expected an exception to be thrown");
    } catch (final Exception thrown) {
      assertEquals(
        String.format("Unexpected exception: %s", thrown.getClass()),
        IllegalArgumentException.class,
        thrown.getClass()
      );

      assertEquals("tags must contain exactly one '=' character", thrown.getMessage());
    }
  }

  @Test
  public void testToString_Serializes() {
    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of(),
      Optional.empty()
    );
    assertEquals("Measurement", measurement.toString());
  }

  @Test
  public void testToString_SerializesWithTags() {
    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of("some", "tags", "here", "i-guess"),
      Optional.empty()
    );
    assertEquals("Measurement,here=i-guess,some=tags", measurement.toString());
  }

  @Test
  public void testToString_SerializesWithField() {
    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of(),
      Optional.of("wow-a-field")
    );
    assertEquals("Measurement wow-a-field", measurement.toString());
  }

  @Test
  public void testToString_SerializesWithTagsAndField() {
    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of("some", "tags", "here", "i-guess"),
      Optional.of("wow-a-field")
    );
    assertEquals("Measurement,here=i-guess,some=tags wow-a-field", measurement.toString());
  }

  @Test
  public void testIntegrated_Serialization() {
    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of(),
      Optional.empty()
    );

    final String measurementStr = measurement.toString();
    assertEquals("Measurement", measurementStr);

    final DropwizardMeasurement fromLine = DropwizardMeasurement.fromLine(measurementStr);
    assertEquals(measurement, fromLine);
  }

  @Test
  public void testIntegrated_SerializationWithTags() {
    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of("action", "restore", "model", "cf-2-005"),
      Optional.empty()
    );

    final String measurementStr = measurement.toString();
    assertEquals("Measurement,action=restore,model=cf-2-005", measurementStr);

    final DropwizardMeasurement fromLine = DropwizardMeasurement.fromLine(measurementStr);
    assertEquals(measurement, fromLine);
  }

  @Test
  public void testIntegrated_SerializationWithField() {
    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of(),
      Optional.of("i-like-being-a-field")
    );

    final String measurementStr = measurement.toString();
    assertEquals("Measurement i-like-being-a-field", measurementStr);

    final DropwizardMeasurement fromLine = DropwizardMeasurement.fromLine(measurementStr);
    assertEquals(measurement, fromLine);
  }

  @Test
  public void testIntegrated_SerializationWithTagsAndField() {
    final DropwizardMeasurement measurement = DropwizardMeasurement.create(
      "Measurement",
      ImmutableMap.of("action", "restore", "model", "cf-2-005"),
      Optional.of("i-like-being-a-field")
    );

    final String measurementStr = measurement.toString();
    assertEquals("Measurement,action=restore,model=cf-2-005 i-like-being-a-field", measurementStr);

    final DropwizardMeasurement fromLine = DropwizardMeasurement.fromLine(measurementStr);
    assertEquals(measurement, fromLine);
  }
}
