package marquez.api.mappers;

import static java.util.stream.Collectors.toList;

import java.util.List;
import lombok.NonNull;
import marquez.api.models.LineageResultResponse;
import marquez.api.models.LineageResultsResponse;
import marquez.service.models.LineageResult;

public final class LineageResultsResponseMapper {
  public static LineageResultResponse map(@NonNull LineageResult lineageResult) {
    return new LineageResultResponse(
        lineageResult.getSubject(),
        lineageResult.getSubjectType(),
        lineageResult.getSubjectNamespace(),
        lineageResult.getPredicate(),
        lineageResult.getObject(),
        lineageResult.getObjectType(),
        lineageResult.getObjectNamespace());
  }

  public static List<LineageResultResponse> map(@NonNull List<LineageResult> lineageResults) {
    return lineageResults.stream().map(lineageResult -> map(lineageResult)).collect(toList());
  }

  public static LineageResultsResponse toLineageResultsResponse(
      @NonNull List<LineageResult> lineageResults) {
    return new LineageResultsResponse(map(lineageResults));
  }
}
