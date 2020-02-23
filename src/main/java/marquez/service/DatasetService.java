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

package marquez.service;

import static java.util.Collections.unmodifiableList;

import io.prometheus.client.Counter;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import marquez.common.models.DatasetUrn;
import marquez.common.models.DatasourceUrn;
import marquez.common.models.NamespaceName;
import marquez.db.DatasetDao;
import marquez.db.DatasourceDao;
import marquez.db.NamespaceDao;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetRowExtended;
import marquez.db.models.DatasourceRow;
import marquez.db.models.DbTableInfoRow;
import marquez.db.models.DbTableVersionRow;
import marquez.db.models.NamespaceRow;
import marquez.gateway.LineageGraphGateway;
import marquez.gateway.exceptions.GatewayException;
import marquez.gateway.models.LineageResultRow;
import marquez.service.exceptions.MarquezServiceException;
import marquez.service.mappers.DatasetMapper;
import marquez.service.mappers.DatasetRowMapper;
import marquez.service.mappers.DatasourceRowMapper;
import marquez.service.mappers.DbTableInfoRowMapper;
import marquez.service.mappers.DbTableVersionRowMapper;
import marquez.service.mappers.LineageResultMapper;
import marquez.service.models.Dataset;
import marquez.service.models.DbTableVersion;
import marquez.service.models.LineageResult;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

@Slf4j
public class DatasetService {
  private static final Counter datasets =
      Counter.build()
          .namespace("marquez")
          .name("dataset_total")
          .labelNames("namespace")
          .help("Total number of datasets.")
          .register();

  private final NamespaceDao namespaceDao;
  private final DatasourceDao datasourceDao;
  private final DatasetDao datasetDao;
  private final LineageGraphGateway lineageGraphGateway;

  public DatasetService(
      @NonNull final NamespaceDao namespaceDao,
      @NonNull final DatasourceDao datasourceDao,
      @NonNull final DatasetDao datasetDao,
      @NonNull final LineageGraphGateway lineageGraphGateway) {
    this.namespaceDao = namespaceDao;
    this.datasourceDao = datasourceDao;
    this.datasetDao = datasetDao;
    this.lineageGraphGateway = lineageGraphGateway;
  }

  public Dataset create(@NonNull NamespaceName namespaceName, @NonNull Dataset dataset)
      throws MarquezServiceException {
    try {
      final NamespaceRow namespaceRow =
          namespaceDao.findBy(namespaceName).orElseThrow(MarquezServiceException::new);
      final DatasourceRow datasourceRow =
          datasourceDao
              .findBy(dataset.getDatasourceUrn())
              .orElseThrow(MarquezServiceException::new);
      final DatasetRow newDatasetRow = DatasetRowMapper.map(namespaceRow, datasourceRow, dataset);
      final DatasetUrn datasetUrn = DatasetUrn.fromString(newDatasetRow.getUrn());
      final Optional<Dataset> datasetIfFound = get(datasetUrn);
      if (datasetIfFound.isPresent()) {
        return datasetIfFound.get();
      }

      final DatasetRow datasetRow =
          datasetDao.insertAndGet(newDatasetRow).orElseThrow(MarquezServiceException::new);
      if (datasetRow.isNew()) datasets.labels(namespaceName.getValue()).inc();
      final DatasourceUrn datasourceUrn = DatasourceUrn.fromString(datasourceRow.getUrn());
      return DatasetMapper.map(datasourceUrn, datasetRow);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to create dataset: {}", dataset, e);
      throw new MarquezServiceException();
    }
  }

  public Dataset create(
      @NonNull NamespaceName namespaceName, @NonNull DbTableVersion dbTableVersion)
      throws MarquezServiceException {
    final DatasourceRow datasourceRow = DatasourceRowMapper.map(dbTableVersion);
    final DatasetRow datasetRow =
        DatasetRowMapper.map(namespaceName, datasourceRow, dbTableVersion);
    final DbTableInfoRow dbTableInfoRow = DbTableInfoRowMapper.map(dbTableVersion);
    final DbTableVersionRow dbTableVersionRow =
        DbTableVersionRowMapper.map(datasetRow, dbTableInfoRow, dbTableVersion);
    try {
      datasetDao.insertAll(datasourceRow, datasetRow, dbTableInfoRow, dbTableVersionRow);
      final Optional<DatasetRowExtended> datasetRowExtendedIfFound =
          datasetDao.findBy(datasetRow.getUuid());
      return datasetRowExtendedIfFound
          .map(DatasetMapper::map)
          .orElseThrow(MarquezServiceException::new);
    } catch (UnableToExecuteStatementException e) {
      log.error(e.getMessage());
      throw new MarquezServiceException();
    }
  }

  public boolean exists(@NonNull DatasetUrn urn) throws MarquezServiceException {
    try {
      return datasetDao.exists(urn);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to check dataset: {}", urn.getValue(), e);
      throw new MarquezServiceException();
    }
  }

  public Optional<Dataset> get(@NonNull DatasetUrn urn) throws MarquezServiceException {
    try {
      return datasetDao.findBy(urn).map(DatasetMapper::map);
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to get dataset: {}", urn.getValue(), e.getMessage());
      throw new MarquezServiceException();
    }
  }

  public Optional<List<LineageResult>> getLineage(@NonNull DatasetUrn urn)
      throws MarquezServiceException {
    try {
      Optional<DatasetRowExtended> dataset = this.datasetDao.findBy(urn);
      if (!dataset.isPresent()) {
        return Optional.empty();
      }
      Optional<NamespaceRow> namespaceRow = namespaceDao.findBy(dataset.get().getNamespaceUuid());
      List<LineageResultRow> lineageResults =
          this.lineageGraphGateway.queryDatasetSubgraph(
              dataset.get(), namespaceRow.get().getName());
      if (lineageResults == null) {
        throw new MarquezServiceException("error no lineage");
      }
      return Optional.of(unmodifiableList(LineageResultMapper.map(lineageResults)));
    } catch (UnableToExecuteStatementException | GatewayException e) {
      log.error("error fetching lineage", e);
      throw new MarquezServiceException();
    }
  }

  public List<Dataset> getAll(
      @NonNull NamespaceName namespaceName, @NonNull Integer limit, @NonNull Integer offset)
      throws MarquezServiceException {
    try {
      final List<DatasetRowExtended> datasetRowsExtended =
          datasetDao.findAll(namespaceName, limit, offset);
      return unmodifiableList(DatasetMapper.map(datasetRowsExtended));
    } catch (UnableToExecuteStatementException e) {
      log.error("Failed to get datasets for namespace: {}", namespaceName.getValue(), e);
      throw new MarquezServiceException();
    }
  }
}
