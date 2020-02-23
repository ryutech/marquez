package marquez.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Getter;
import lombok.ToString;

@ToString
public class LineageResultsResponse {
  @Getter private final List<LineageResultResponse> results;

  @JsonCreator
  public LineageResultsResponse(@JsonProperty("result") List<LineageResultResponse> results) {
    this.results = results;
  }
}
