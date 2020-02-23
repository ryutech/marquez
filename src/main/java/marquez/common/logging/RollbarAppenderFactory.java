/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marquez.common.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.Appender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.tapstream.rollbar.RollbarAppender;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;
import javax.validation.constraints.NotNull;

@JsonTypeName("rollbar")
public class RollbarAppenderFactory extends AbstractAppenderFactory {

  @NotNull private String environment;
  private String apiKey;

  @JsonProperty
  public void setEnvironment(String environment) {
    this.environment = environment;
  }

  @JsonProperty
  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  @JsonProperty
  public String getEnvironment() {
    return environment;
  }

  @JsonProperty
  public String getApiKey() {
    return apiKey;
  }

  @Override
  public Appender build(
      LoggerContext context,
      String applicationName,
      LayoutFactory layoutFactory,
      LevelFilterFactory levelFilterFactory,
      AsyncAppenderFactory asyncAppenderFactory) {
    final RollbarAppender appender = new RollbarAppender();
    appender.setApiKey(apiKey);
    appender.setEnvironment(environment);
    appender.setContext(context);
    appender.setName(applicationName);
    appender.start();

    return appender;
  }
}
