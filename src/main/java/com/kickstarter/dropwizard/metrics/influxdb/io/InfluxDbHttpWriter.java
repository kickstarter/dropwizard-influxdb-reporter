package com.kickstarter.dropwizard.metrics.influxdb.io;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.client.JerseyClientConfiguration;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.Range;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.concurrent.Executors;

/**
 * An {@link InfluxDbWriter} that writes to an HTTP/S server using a {@link Client}.
 */
public class InfluxDbHttpWriter implements InfluxDbWriter {
  private final WebTarget influxLines;

  public InfluxDbHttpWriter(final Client client, final String endpoint) {
    this.influxLines = client.target(endpoint);
  }

  @Override
  public void writeBytes(final byte[] bytes) {
    influxLines.request().post(Entity.entity(bytes, MediaType.APPLICATION_OCTET_STREAM_TYPE));
  }

  @Override
  public void close() throws IOException {} // NOOP

  // ===================================================================================================================
  // Builder

  /**
   * A factory for {@link InfluxDbHttpWriter}.
   * <p/>
   * <b>Configuration Parameters:</b>
   * <table>
   *     <tr>
   *         <td>Name</td>
   *         <td>Default</td>
   *         <td>Description</td>
   *     </tr>
   *     <tr>
   *         <td>host</td>
   *         <td><i>localhost</i></td>
   *         <td>the InfluxDB hostname.</td>
   *     </tr>
   *     <tr>
   *         <td>port</td>
   *         <td><i>8086</i></td>
   *         <td>the InfluxDB port.</td>
   *     </tr>
   *     <tr>
   *         <td>database</td>
   *         <td><i>none</i></td>
   *         <td>the database to write to.</td>
   *     </tr>
   *     <tr>
   *         <td>jersey</td>
   *         <td><i>default</i></td>
   *         <td>the jersey client configuration.</td>
   *     </tr>
   * </table>
   */
  public static class Factory implements InfluxDbWriter.Factory {
    @NotBlank
    @JsonProperty
    private String host = "localhost";

    @Range(min = 0, max = 49151)
    @JsonProperty
    private int port = 8086;

    @JsonProperty
    private JerseyClientConfiguration jersey = new JerseyClientConfiguration();

    @NotBlank
    @JsonProperty
    private String database;

    public InfluxDbWriter build(final MetricRegistry metrics) {
      final Client client = new io.dropwizard.client.JerseyClientBuilder(metrics)
        .using(jersey)
        .using(new ObjectMapper())
        .using(Executors.newSingleThreadExecutor())
        .build("influxdb-http-writer");

      try {
        final String query = "/write?db=" + URLEncoder.encode(database, "UTF-8");
        final URL endpoint = new URL("http", host, port, query);
        return new InfluxDbHttpWriter(client, endpoint.toString());
      } catch (MalformedURLException | UnsupportedEncodingException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
