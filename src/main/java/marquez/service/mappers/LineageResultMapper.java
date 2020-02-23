package marquez.service.mappers;

import static java.util.stream.Collectors.toList;

import java.util.List;
import lombok.NonNull;
import marquez.gateway.models.LineageResultRow;
import marquez.service.models.LineageResult;

public final class LineageResultMapper {
  public static LineageResult map(@NonNull LineageResultRow lineageResult) {
    return new LineageResult(
        lineageResult.getSubject(),
        lineageResult.getSubjectType(),
        lineageResult.getSubjectNamespace(),
        lineageResult.getPredicate(),
        lineageResult.getObject(),
        lineageResult.getObjectType(),
        lineageResult.getObjectNamespace());
  }

  public static List<LineageResult> map(@NonNull List<LineageResultRow> lineageResults) {
    return lineageResults.stream().map(lineageResult -> map(lineageResult)).collect(toList());
  }
}
