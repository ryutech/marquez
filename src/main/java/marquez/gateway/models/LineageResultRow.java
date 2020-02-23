package marquez.gateway.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

@ToString
public class LineageResultRow {
  @Getter private final String subject;
  @Getter private final String subjectType;
  @Getter private final String subjectNamespace;
  @Getter private final String predicate;
  @Getter private final String object;
  @Getter private final String objectType;
  @Getter private final String objectNamespace;

  public LineageResultRow(
      @JsonProperty("subject") String subject,
      @JsonProperty("subject_type") String subjectType,
      @JsonProperty("subject_namespace") String subjectNamespace,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("object") String object,
      @JsonProperty("object_type") String objectType,
      @JsonProperty("object_namespace") String objectNamespace) {
    this.subject = subject;
    this.subjectType = subjectType;
    this.subjectNamespace = subjectNamespace;
    this.predicate = predicate;
    this.object = object;
    this.objectType = objectType;
    this.objectNamespace = objectNamespace;
  }
}
