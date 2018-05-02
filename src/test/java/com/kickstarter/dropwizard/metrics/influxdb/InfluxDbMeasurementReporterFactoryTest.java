package com.kickstarter.dropwizard.metrics.influxdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.kickstarter.dropwizard.metrics.influxdb.io.InfluxDbHttpWriter;
import com.kickstarter.dropwizard.metrics.influxdb.io.InfluxDbTcpWriter;
import com.kickstarter.dropwizard.metrics.influxdb.io.InfluxDbWriter;
import com.kickstarter.dropwizard.metrics.influxdb.transformer.TaggedPattern;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InfluxDbMeasurementReporterFactoryTest {
  
  @Test
  public void testTcpSerialization() throws IOException {
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

    final InfluxDbWriter.Factory factoryImpl = factory.sender();
    assertThat("expected TCP implementation", factoryImpl, CoreMatchers.instanceOf(InfluxDbTcpWriter.Factory.class));
  
    InfluxDbTcpWriter.Factory tcp = (InfluxDbTcpWriter.Factory) factoryImpl;
    assertEquals("expected TCP host", "i am a host", tcp.host());
    assertEquals("expected TCP port", 12345, tcp.port());
    assertEquals("expected TCP timeout", Duration.minutes(5), tcp.timeout());
  }
  
  
  @Test
  public void testHttpSerialization() throws IOException {
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
                "\"type\": \"http\"," +
                "\"host\": \"i am a host\"," +
                "\"port\": \"12345\"," +
                "\"database\": \"database name\"," +
                "\"jersey\": {" +
                  "\"timeout\": \"5 minutes\"," +
                  "\"connectionTimeout\": \"2 minutes\"" +
                "}" +
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
    
    final InfluxDbWriter.Factory factoryImpl = factory.sender();
    assertThat("expected http implementation", factoryImpl, CoreMatchers.instanceOf(InfluxDbHttpWriter.Factory.class));
    
    InfluxDbHttpWriter.Factory http = (InfluxDbHttpWriter.Factory) factoryImpl;
    assertEquals("expected TCP host", "i am a host", http.host());
    assertEquals("expected http port", 12345, http.port());
    assertEquals("expected http timeout", Duration.minutes(5), http.jersey().getTimeout());
    assertEquals("expected http connect timeout", Duration.minutes(2), http.jersey().getConnectionTimeout());
    assertEquals("expected database name ", "database name", http.database());
  }
}
