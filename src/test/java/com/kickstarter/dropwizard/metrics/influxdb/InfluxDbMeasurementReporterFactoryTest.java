package com.kickstarter.dropwizard.metrics.influxdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.kickstarter.dropwizard.metrics.influxdb.io.InfluxDbTcpWriter;
import com.kickstarter.dropwizard.metrics.influxdb.transformer.TaggedPattern;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InfluxDbMeasurementReporterFactoryTest {
  @Test
  public void testSerialization() throws IOException {
    final String json =
      "{" +
        "\"type\": \"influxdb\"," +
        "\"globalTags\": {\"some\": \"tag\"}," +
        "\"metricTemplates\": {" +
          "\"measurement\": {" +
            "\"pattern\": \".*blah\"," +
            "\"tagKeys\": [\"blah\"]" +
          "}" +
        "}," +
        "\"groupGauges\": true," +
        "\"groupCounters\": true," +
        "\"sender\": {" +
          "\"type\": \"tcp\"," +
          "\"host\": \"i am a host\"," +
          "\"port\": \"12345\"," +
          "\"timeout\": \"5 minutes\"" +
        "}" +
      "}";

    final ObjectMapper mapper = Jackson.newObjectMapper();
    mapper.registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));

    final Environment env = mock(Environment.class);
    when(env.getObjectMapper()).thenReturn(mapper);

    final InfluxDbMeasurementReporterFactory factory = mapper.readValue(json, InfluxDbMeasurementReporterFactory.class);
    assertEquals("expected global tags", ImmutableMap.of("some", "tag"), factory.globalTags());

    final Map<String, TaggedPattern> templates = factory.metricTemplates();
    assertEquals("expected a single template", 1, templates.size());

    final TaggedPattern template = templates.get("measurement");
    assertEquals("expected template pattern", ".*blah", template.pattern());
    assertEquals("expected template tag keys", ImmutableList.of("blah"), template.tagKeys());


    assertTrue("expected group_guages", factory.groupGauges());
    assertTrue("expected group_counters", factory.groupCounters());

    final InfluxDbTcpWriter.Factory tcp = (InfluxDbTcpWriter.Factory) factory.sender();
    assertEquals("expected TCP host", "i am a host", tcp.host());
    assertEquals("expected TCP port", 12345, tcp.port());
    assertEquals("expected TCP timeout", Duration.minutes(5), tcp.timeout());
  }
}
