package com.kickstarter.dropwizard.metrics.influxdb.transformer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.constraints.NotNull;
import org.hibernate.validator.constraints.NotBlank;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toMap;

/**
 * A wrapper for {@link Pattern regex patterns}, used for extracting tags.
 */
public class TaggedPattern {
  @NotBlank
  @JsonProperty
  private String pattern;
  public String pattern() {
    return pattern;
  }

  @NotNull
  @JsonProperty
  private List<String> tagKeys;
  public List<String> tagKeys() {
    return tagKeys;
  }

  // for internal use.
  private final Pattern compiledPattern;

  @JsonCreator
  public TaggedPattern(final String pattern, final List<String> tagKeys) {
    this.pattern = pattern;
    this.tagKeys = tagKeys;
    this.compiledPattern = Pattern.compile(pattern);
  }

  @VisibleForTesting TaggedPattern(final String pattern, final String... tagKeys) {
    this(pattern, Arrays.asList(tagKeys));
  }

  /**
   * Matches the {@code input} string and returns a map of matched tags.
   * The tag map will not contain any empty pattern matcher groups.
   *
   * @return An Optional-wrapped mapping of tags, or {@link Optional#empty()}
   * if the input does not match the pattern.
   */
  /*package*/ Optional<Map<String, String>> tags(final CharSequence input) {
    final Matcher matcher = compiledPattern.matcher(input);
    if (matcher.matches()) {
      return Optional.of(tagKeys.stream()
        .filter(tag -> matcher.group(tag) != null)
        .filter(tag -> !matcher.group(tag).isEmpty())
        .collect(toMap(Function.identity(), matcher::group)));
    }
    return Optional.empty();
  }
}
