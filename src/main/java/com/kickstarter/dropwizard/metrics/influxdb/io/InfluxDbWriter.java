package com.kickstarter.dropwizard.metrics.influxdb.io;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.IOException;

/**
 * Writes bytes to an InfluxDB input.
 */
public interface InfluxDbWriter {
  /**
   * Write the given bytes to the connection.
   *
   * @throws Exception if an error occurs while writing.
   */
  void writeBytes(final byte[] bytes) throws Exception;
  /**
   * Close the writer connection, if it is open.
   *
   * @throws IOException if an I/O error occurs when closing the connection.
   */
  void close() throws IOException;

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = InfluxDbHttpWriter.Factory.class, name = "http"),
    @JsonSubTypes.Type(value = InfluxDbTcpWriter.Factory.class, name = "tcp")})
  interface Factory {
    InfluxDbWriter build(final MetricRegistry metrics);
  }
}
