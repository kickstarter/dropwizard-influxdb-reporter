package com.kickstarter.dropwizard.metrics.influxdb;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.kickstarter.dropwizard.metrics.influxdb.io.InfluxDbHttpWriter;
import com.kickstarter.dropwizard.metrics.influxdb.io.InfluxDbWriter;
import com.kickstarter.dropwizard.metrics.influxdb.io.Sender;
import com.kickstarter.dropwizard.metrics.influxdb.transformer.DropwizardMeasurementParser;
import com.kickstarter.dropwizard.metrics.influxdb.transformer.DropwizardTransformer;
import com.kickstarter.dropwizard.metrics.influxdb.transformer.TaggedPattern;
import io.dropwizard.metrics.BaseReporterFactory;
import javax.validation.constraints.NotNull;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory for {@link InfluxDbMeasurementReporter} instances.
 * <p/>
 * <b>Configuration Parameters:</b>
 * <table>
 *     <tr>
 *         <td>Name</td>
 *         <td>Default</td>
 *         <td>Description</td>
 *     </tr>
 *     <tr>
 *         <td>globalTags</td>
 *         <td><i>None</i></td>
 *         <td>tags for all metrics reported to InfluxDb.</td>
 *     </tr>
 *     <tr>
 *       <td>metricTemplates</td>
 *       <td>None</td>
 *       <td>tagged metric templates for converting names passed through MetricRegistry.</td>
 *     </tr>
 *     <tr>
 *         <td>groupGauges</td>
 *         <td><i>true</i></td>
 *         <td>A boolean to signal whether to group gauges when reporting.</td>
 *     </tr>
 *     <tr>
 *         <td>groupCounters</td>
 *         <td><i>true</i></td>
 *         <td>A boolean to signal whether to group counters when reporting.</td>
 *     </tr>
 *     <tr>
 *       <td>sender</td>
 *       <td>http</td>
 *       <td>The type and configuration for reporting measurements to a receiver.</td>
 *     </tr>
 * </table>
 */
@JsonTypeName("influxdb")
public class InfluxDbMeasurementReporterFactory extends BaseReporterFactory {
  @NotNull
  @JsonProperty
  private Map<String, String> globalTags = new HashMap<>();
  @VisibleForTesting Map<String, String> globalTags() {
    return globalTags;
  }

  @NotNull
  @JsonProperty
  private Map<String, TaggedPattern> metricTemplates = new HashMap<>();
  @VisibleForTesting Map<String, TaggedPattern> metricTemplates() {
    return metricTemplates;
  }

  @NotNull
  @JsonProperty
  private boolean groupGauges = true;
  @VisibleForTesting boolean groupGauges() {
    return groupGauges;
  }

  @NotNull
  @JsonProperty
  private boolean groupCounters = true;
  @VisibleForTesting boolean groupCounters() {
    return groupCounters;
  }

  @NotNull
  @JsonProperty
  private InfluxDbWriter.Factory sender = new InfluxDbHttpWriter.Factory();
  @VisibleForTesting InfluxDbWriter.Factory sender() {
    return sender;
  }

  @Override
  public ScheduledReporter build(final MetricRegistry registry) {
    final Sender builtSender = new Sender(sender.build(registry));
    final DropwizardTransformer transformer = new DropwizardTransformer(
      globalTags,
      DropwizardMeasurementParser.withTemplates(metricTemplates),
      groupCounters,
      groupGauges,
      getRateUnit(),
      getDurationUnit()
    );

    return new InfluxDbMeasurementReporter(
      builtSender,
      registry,
      getFilter(),
      getRateUnit(),
      getDurationUnit(),
      Clock.systemUTC(),
      transformer
    );
  }
}
