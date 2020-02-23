package marquez.gateway.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

@ToString
public class CayleyTriple {
  @Getter private final String subject;
  @Getter private final String predicate;
  @Getter private final String object;

  public CayleyTriple(
      @JsonProperty("subject") String subject,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("object") String object) {
    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
  }
}
