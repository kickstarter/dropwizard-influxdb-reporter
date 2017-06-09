package com.kickstarter.dropwizard.metrics.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InfluxDbMeasurementTest {
  @Test
  public void testCreate_EmptyName() {
    try {
      InfluxDbMeasurement.create("", ImmutableMap.of(), ImmutableMap.of("some", "field"), 123456L);
      fail("Expected an Exception to be thrown");
    } catch (final Exception thrown) {
      assertEquals("Expected an IllegalArgumentException", IllegalArgumentException.class, thrown.getClass());
      assertEquals("InfluxDbMeasurement must contain a non-empty name", thrown.getMessage());
    }
  }

  @Test
  public void testCreate_EmptyFields() {
    try {
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of(), 123456L);
      fail("Expected an Exception to be thrown");
    } catch (final Exception thrown) {
      assertEquals("Expected an IllegalArgumentException", IllegalArgumentException.class, thrown.getClass());
      assertEquals("InfluxDbMeasurement must contain at least one field", thrown.getMessage());
    }
  }

  @Test
  public void testToLine_WithNoTags() {
    final InfluxDbMeasurement Measurement = InfluxDbMeasurement.create(
      "Measurement", ImmutableMap.of(), ImmutableMap.of("val", "5i"), 1304695L
    );
    assertEquals("Measurement val=5i 1304695000000", Measurement.toLine());
  }

  @Test
  public void testToLine_WithSingleTag() {
    final InfluxDbMeasurement Measurement = InfluxDbMeasurement.create(
      "Measurement", ImmutableMap.of("action", "restore"), ImmutableMap.of("val", "5i"), 1304695L
    );
    assertEquals("Measurement,action=restore val=5i 1304695000000", Measurement.toLine());
  }

  @Test
  public void testToLine_WithMultipleTags() {
    final InfluxDbMeasurement Measurement = InfluxDbMeasurement.create(
      "Measurement", ImmutableMap.of("action", "restore", "model", "cf-2-005"), ImmutableMap.of("val", "5i"), 1304695L
    );
    assertEquals("Measurement,action=restore,model=cf-2-005 val=5i 1304695000000", Measurement.toLine());
  }

  @Test
  public void testToLine_WithMultipleFields() {
    final InfluxDbMeasurement Measurement = InfluxDbMeasurement.create(
      "Measurement", ImmutableMap.of(), ImmutableMap.of("val", "5i", "other-val", "true"), 1304695L
    );
    assertEquals("Measurement val=5i,other-val=true 1304695000000", Measurement.toLine());
  }

  @Test
  public void testToLine_WithMultipleTagsAndFields() {
    final InfluxDbMeasurement Measurement = InfluxDbMeasurement.create(
      "Measurement",
      ImmutableMap.of("action", "restore", "model", "cf-2-005"),
      ImmutableMap.of("val", "5i", "other-val", "true"),
      1304695L
    );

    assertEquals(
      "Measurement,action=restore,model=cf-2-005 val=5i,other-val=true 1304695000000",
      Measurement.toLine());
  }

  @Test
  public void testJoinPairs_WithNoPairs() {
    assertTrue("should return an empty string", InfluxDbMeasurement.joinPairs(ImmutableMap.of()).isEmpty());
  }

  @Test
  public void testJoinPairs_WithOnePair() {
    final String joined = InfluxDbMeasurement.joinPairs(ImmutableMap.of("action", "restore"));
    assertEquals("should join a pair without commas", "action=restore", joined);
  }

  @Test
  public void testJoinPairs_WithMultiplePairs() {
    final String joined = InfluxDbMeasurement.joinPairs(ImmutableMap.of("action", "restore", "greetings", "pleasings"));
    assertEquals("should join pairs with commas", "action=restore,greetings=pleasings", joined);
  }

  @Test
  public void testBuilder_PutTags() {
    final Map<String, String> tags = ImmutableMap.of("pleased", "to", "meet", "you");
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putTags(tags)
      .putField("welcome", "manatee")
      .build();

    assertEquals("should add all tags to the measurement",
      InfluxDbMeasurement.create("Measurement", tags, ImmutableMap.of("welcome", "manatee"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_IsInvalidWithEmptyName() {
    final InfluxDbMeasurement.Builder measurement = new InfluxDbMeasurement.Builder("", 90210L)
      .putField("some-float", 15321F);

    assertFalse("should be invalid with empty name", measurement.isValid());
  }

  @Test
  public void testBuilder_IsInvalidWithEmptyFields() {
    final InfluxDbMeasurement.Builder measurement = new InfluxDbMeasurement.Builder("hi", 90210L)
      .tryPutFields(ImmutableMap.of("some-float", Float.NaN), __ -> {});

    assertFalse("should be invalid with empty fields", measurement.isValid());
  }

  @Test
  public void testBuilder_PutFloatField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-float", 15321F)
      .build();

    assertEquals("should add double field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("some-float", "15321.0"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutNanFloatField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-float", Float.NaN)
      .putField("need", "val")
      .build();

    assertEquals("should not add NaN float field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("need", "val"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutInfiniteFloatField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-float", Float.POSITIVE_INFINITY)
      .putField("need", "val")
      .build();

    assertEquals("should not add infinite float field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("need", "val"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutDoubleField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-double", 15321D)
      .build();

    assertEquals("should add formatted double field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("some-double", "15321.0"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutNanDoubleField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-double", Double.NaN)
      .putField("need", "val")
      .build();

    assertEquals("should not add NaN double field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("need", "val"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutInfiniteDoubleField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-double", Double.POSITIVE_INFINITY)
      .putField("need", "val")
      .build();

    assertEquals("should not add infinite double field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("need", "val"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutIntegerField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-int", 15321)
      .build();

    assertEquals("should add integer field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("some-int", "15321i"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutLongField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-long", 15321L)
      .build();

    assertEquals("should add long field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("some-long", "15321i"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutStringField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("some-str", "cookies")
      .build();

    assertEquals("should add String field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("some-str", "cookies"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutBooleanField() {
    final InfluxDbMeasurement measurement = new InfluxDbMeasurement.Builder("Measurement", 90210L)
      .putField("eating", true)
      .build();

    assertEquals("should add Boolean field to the measurement",
      InfluxDbMeasurement.create("Measurement", ImmutableMap.of(), ImmutableMap.of("eating", "true"), 90210L),
      measurement);
  }

  @Test
  public void testBuilder_PutNonStringOrPrimitiveField() {
    try {
      new InfluxDbMeasurement.Builder("Measurement", 90210L).putField("val", Arrays.asList(1, 2, 3));
      fail("Expected an Exception when adding a non-String or -primitive field");
    } catch (final Exception thrown) {
      assertEquals("Expected an IllegalArgumentException", IllegalArgumentException.class, thrown.getClass());
      assertEquals("InfluxDbMeasurement field 'val' must be String or primitive: [1, 2, 3]", thrown.getMessage());
    }
  }
  
  @Test
  public void testBuilder_TryPutFields() {
    final ArrayList<Exception> exceptions = new ArrayList<>();

    new InfluxDbMeasurement.Builder("Measurement", 90210L).tryPutFields(ImmutableMap.of(
      "a", ImmutableSet.of(),
      "c", "d",
      "e", Arrays.asList(1, 2, 3)
    ), exceptions::add);

    assertEquals("should catch two exceptions", 2, exceptions.size());
    assertEquals("should catch IllegalArgumentExceptions", IllegalArgumentException.class, exceptions.get(0).getClass());
    assertEquals("should catch IllegalArgumentExceptions", IllegalArgumentException.class, exceptions.get(1).getClass());
    assertEquals("should describe the invalid key-value pair",
      ImmutableList.of(
        "InfluxDbMeasurement field 'a' must be String or primitive: []",
        "InfluxDbMeasurement field 'e' must be String or primitive: [1, 2, 3]"
      ),
      exceptions.stream().map(Exception::getMessage).collect(toList())
    );
  }
}
