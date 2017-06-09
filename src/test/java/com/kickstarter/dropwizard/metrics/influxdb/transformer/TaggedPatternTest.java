package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class TaggedPatternTest {
  private static final TaggedPattern taggedPattern = new TaggedPattern(
    "\\.hello\\.(?<action>[A-Za-z]+)\\.?(?<model>.*)",
    "action", "model"
  );

  @Test
  public void testTags_noMatch() {
    assertEquals(
      Optional.empty(),
      taggedPattern.tags(".hello.123")
    );
  }

  @Test
  public void testTags_partialMatch() {
    assertEquals(
      Optional.of(ImmutableMap.of("action", "restore")),
      taggedPattern.tags(".hello.restore")
    );
  }

  @Test
  public void testTags_fullMatch() {
    assertEquals(
      Optional.of(ImmutableMap.of("action", "restore", "model", "collab-2-005")),
      taggedPattern.tags(".hello.restore.collab-2-005")
    );
  }

  @Test
  public void testTags_matchWithoutTags() {
    final TaggedPattern untaggedPattern = new TaggedPattern("\\.hello\\..*\\.whatever");
    assertEquals(
      Optional.of(ImmutableMap.of()),
      untaggedPattern.tags(".hello.anything.whatever")
    );
  }
}
