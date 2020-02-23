package marquez.gateway.models;

import java.util.Random;

public final class GatewayModelGenerator {
  private static final Random RANDOM = new Random();

  public static LineageResultRow newLineageResultRow() {
    return new LineageResultRow(
        String.format("job%d", newId()),
        "job",
        "testNamespace",
        "to",
        String.format("dataset%d", newId()),
        "dataset",
        "testNamespace");
  }

  private static int newId() {
    return RANDOM.nextInt(Integer.MAX_VALUE - 1);
  }
}
