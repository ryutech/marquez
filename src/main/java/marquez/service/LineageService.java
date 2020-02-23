package marquez.service;

import lombok.extern.slf4j.Slf4j;
import marquez.gateway.LineageGraphGateway;

@Slf4j
public class LineageService {
  private final LineageGraphGateway lineageGraphGateway;

  public LineageService(LineageGraphGateway lineageGraphGateway) {
    this.lineageGraphGateway = lineageGraphGateway;
  }

  public void linkJobToDataset() {}
}
