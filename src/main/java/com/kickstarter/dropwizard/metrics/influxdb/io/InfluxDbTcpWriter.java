package com.kickstarter.dropwizard.metrics.influxdb.io;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Duration;
import javax.validation.constraints.NotNull;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.Range;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * An {@link InfluxDbWriter} that writes bytes to TCP sockets.
 */
public class InfluxDbTcpWriter implements InfluxDbWriter {
  private final String host;
  private final int port;
  private final Duration timeout;
  private Socket tcpSocket;

  public InfluxDbTcpWriter(final String host, final int port, final Duration timeout) {
    this.host = host;
    this.port = port;
    this.timeout = timeout;
  }

  @Override
  public void writeBytes(final byte[] bytes) throws IOException {
    if (tcpSocket == null) {
      tcpSocket = new Socket(host, port);
      tcpSocket.setSoTimeout((int) timeout.toMilliseconds());
    }

    final OutputStream outputStream = tcpSocket.getOutputStream();
    outputStream.write(bytes);
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (tcpSocket != null) {
      tcpSocket.close();
      tcpSocket = null;
    }
  }

  // ===================================================================================================================
  // Builder

  /**
   * A factory for {@link InfluxDbTcpWriter}.
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
   *         <td>the consumer hostname.</td>
   *     </tr>
   *     <tr>
   *         <td>port</td>
   *         <td><i>8086</i></td>
   *         <td>the consumer port.</td>
   *     </tr>
   *     <tr>
   *         <td>timeout</td>
   *         <td><i>500 milliseconds/i></td>
   *         <td>the socket timeout duration.</td>
   *     </tr>
   * </table>
   */
  public static class Factory implements InfluxDbWriter.Factory {
    @NotBlank
    @JsonProperty
    private String host = "localhost";
    public String host() {
      return host;
    }

    @Range(min = 0, max = 49151)
    @JsonProperty
    private int port = 8086;
    public int port() {
      return port;
    }

    @NotNull
    @JsonProperty
    private Duration timeout = Duration.milliseconds(500);
    public Duration timeout() {
      return timeout;
    }

    @Override public InfluxDbWriter build(final MetricRegistry __) {
      return new InfluxDbTcpWriter(host, port, timeout);
    }
  }
}
