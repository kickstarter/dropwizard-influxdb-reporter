package com.kickstarter.dropwizard.metrics.influxdb.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.kickstarter.dropwizard.metrics.influxdb.InfluxDbMeasurement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SenderTest {
  @Test
  public void testSend_SendsMeasures() throws Exception {
    final InfluxDbWriter writer = mock(InfluxDbWriter.class);
    final Sender sender = new Sender(writer);
    sender.send(ImmutableList.of(
      InfluxDbMeasurement.create("hello", ImmutableMap.of("x", "y"), ImmutableMap.of("a", "b"), 90210L),
      InfluxDbMeasurement.create("hello", ImmutableMap.of("x", "y"), ImmutableMap.of("e", "d"), 90210L)
    ));

    verify(writer, only()).writeBytes("hello,x=y a=b 90210000000\nhello,x=y e=d 90210000000\n".getBytes());
    assertEquals("should clear measure queue", 0, sender.queuedMeasures());
  }

  @Test
  public void testSend_EmptyMeasures() throws Exception {
    final InfluxDbWriter writer = mock(InfluxDbWriter.class);
    final Sender sender = new Sender(writer);
    sender.send(ImmutableList.of());

    verify(writer, never()).writeBytes(any());
    verify(writer, never()).close();
  }

  @Test
  public void testSend_HandlesExceptions() throws Exception {
    final InfluxDbWriter writer = mock(InfluxDbWriter.class);
    final Sender sender = new Sender(writer);

    doThrow(new RuntimeException("what did you do")).when(writer).writeBytes(any());
    sender.send(ImmutableList.of(
      InfluxDbMeasurement.create("hello", ImmutableMap.of("x", "y"), ImmutableMap.of("a", "b"), 90210L))
    );

    verify(writer, times(1)).writeBytes("hello,x=y a=b 90210000000\n".getBytes());
    verify(writer, times(1)).close();
  }

  @Test
  public void testSend_RetriesSend() throws Exception {
    final InfluxDbWriter writer = mock(InfluxDbWriter.class);
    final Sender sender = new Sender(writer);

    doThrow(new RuntimeException("what did you do"))
      .doNothing().when(writer).writeBytes(any());

    sender.send(ImmutableList.of(
      InfluxDbMeasurement.create("hello", ImmutableMap.of("x", "y"), ImmutableMap.of("a", "b"), 90210L))
    );

    verify(writer, times(1)).writeBytes("hello,x=y a=b 90210000000\n".getBytes());
    verify(writer, times(1)).close();

    sender.send(ImmutableList.of());
    verify(writer, times(2)).writeBytes("hello,x=y a=b 90210000000\n".getBytes());
  }
}
