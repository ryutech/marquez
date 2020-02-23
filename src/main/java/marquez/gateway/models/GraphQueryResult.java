package marquez.gateway.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Getter;
import lombok.ToString;

@ToString
public class GraphQueryResult {
  @Getter private final List<LineageResultRow> results;

  @JsonCreator
  public GraphQueryResult(@JsonProperty("result") List<LineageResultRow> results) {
    this.results = results;
  }
}
