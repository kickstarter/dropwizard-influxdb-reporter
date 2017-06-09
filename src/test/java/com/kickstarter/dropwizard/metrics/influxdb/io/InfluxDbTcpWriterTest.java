package com.kickstarter.dropwizard.metrics.influxdb.io;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InfluxDbTcpWriterTest {
  @Test
  public void testSerialization() throws IOException {
    final String json =
      "{" +
        "\"type\": \"tcp\"," +
        "\"host\": \"i am a host\"," +
        "\"port\": \"12345\"," +
        "\"timeout\": \"5 minutes\"" +
      "}";

    final ObjectMapper mapper = Jackson.newObjectMapper();
    mapper.registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));

    final Environment env = mock(Environment.class);
    when(env.getObjectMapper()).thenReturn(mapper);

    final InfluxDbTcpWriter.Factory factory = mapper.readValue(json, InfluxDbTcpWriter.Factory.class);
    assertEquals("expected TCP host", "i am a host", factory.host());
    assertEquals("expected TCP port", 12345, factory.port());
    assertEquals("expected TCP timeout", Duration.minutes(5), factory.timeout());
  }
}
