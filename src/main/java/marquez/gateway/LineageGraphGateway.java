package marquez.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import marquez.db.models.DatasetRowExtended;
import marquez.gateway.exceptions.GatewayException;
import marquez.gateway.models.CayleyTriple;
import marquez.gateway.models.GraphQueryResult;
import marquez.gateway.models.LineageResultRow;
import marquez.service.models.Job;

@Slf4j
public class LineageGraphGateway {
  private String host;
  private int port;
  private static String GIZMO_QUERY_PATH = "api/v1/query/gizmo";
  private static String QUAD_WRITE_PATH = "api/v1/write";
  private static final int DEFAULT_SUBGRAPH_DEPTH = 5;
  private ObjectReader queryResultMapper;
  private ObjectWriter tripleWriter;
  private static final String TO_PREDICATE = "to";
  private static final String TYPE_PREDICATE = "type";
  private static final String JOB_TYPE = "job";
  private static final String DATASET_TYPE = "dataset";
  private static final String NAMESPACE_PREDICATE = "namespace";
  private static final String NAME_PREDICATE = "name";

  public LineageGraphGateway(String host, int port) {
    this.host = host;
    this.port = port;
    this.queryResultMapper = new ObjectMapper().readerFor(GraphQueryResult.class);
    this.tripleWriter = new ObjectMapper().writerFor(CayleyTriple.class);
  }

  private String buildNodeId(String namespace, String uuid, String type) {
    final String SEPARATOR = "\u241F";
    return String.format("%s", namespace + SEPARATOR + uuid + SEPARATOR + type);
  }

  public List<LineageResultRow> queryDatasetSubgraph(DatasetRowExtended dataset, String namespace)
      throws GatewayException {
    String resultJson =
        queryNodeSubgraph(buildNodeId(namespace, dataset.getUuid().toString(), "dataset"));
    try {
      Optional<GraphQueryResult> result = Optional.of(this.queryResultMapper.readValue(resultJson));
      if (result.isPresent()) {
        return result.get().getResults();
      }
    } catch (IOException e) {
      throw new GatewayException("error parsing cayley result", e);
    }
    return new ArrayList<LineageResultRow>();
  }

  public List<LineageResultRow> queryJobSubgraph(Job job, String namespace)
      throws GatewayException {
    String resultJson = queryNodeSubgraph(buildNodeId(namespace, job.getGuid().toString(), "job"));
    try {
      Optional<GraphQueryResult> result = Optional.of(this.queryResultMapper.readValue(resultJson));
      if (result.isPresent()) {
        return result.get().getResults();
      }
    } catch (IOException e) {
      throw new GatewayException("error parsing cayley result", e);
    }
    return new ArrayList<LineageResultRow>();
  }

  private String queryNodeSubgraph(String nodeName) throws GatewayException {
    try {
      return doGizmoQuery(buildGizmoSubgraphQuery(nodeName, DEFAULT_SUBGRAPH_DEPTH));
    } catch (IOException e) {
      throw new GatewayException("failed to get subgraph for node", e);
    }
  }

  private static String buildGizmoSubgraphQuery(String nodeName, int depth) {
    String query =
        "g.V(\"%1$s\").Out(\"to\").ForEach(\n"
            + "function(v){\n"
            + "    var nodeChild = {\n"
            + "      subject: g.V(\"%1$s\").Out(\"name\").ToValue(),\n"
            + "      subject_type: g.V(\"%1$s\").Out(\"type\").ToValue(),\n"
            + "      subject_namespace: g.V(\"%1$s\").Out(\"namespace\").ToValue(),\n"
            + "      predicate: \"to\",\n"
            + "      object: g.V(v.id).Out(\"name\").ToValue(),\n"
            + "      object_type: g.V(v.id).Out(\"type\").ToValue(),\n"
            + "      object_namespace: g.V(v.id).Out(\"namespace\").ToValue(),\n"
            + "    }\n"
            + "    g.Emit(nodeChild)\n"
            + "  }\n"
            + ")\n"
            + "g.V(\"%1$s\").In(\"to\").ForEach(\n"
            + "function(v){\n"
            + "    var nodeChild = {\n"
            + "      object: g.V(\"%1$s\").Out(\"name\").ToValue(),\n"
            + "      object_type: g.V(\"%1$s\").Out(\"type\").ToValue(),\n"
            + "      object_namespace: g.V(\"%1$s\").Out(\"namespace\").ToValue(),\n"
            + "      predicate: \"to\",\n"
            + "      subject: g.V(v.id).Out(\"name\").ToValue(),\n"
            + "      subject_type: g.V(v.id).Out(\"type\").ToValue(),\n"
            + "      subject_namespace: g.V(v.id).Out(\"namespace\").ToValue(),\n"
            + "    }\n"
            + "    g.Emit(nodeChild)\n"
            + "  }\n"
            + ")\n"
            + "g.V(\"%1$s\").FollowRecursive(g.M().Out(\"to\"), %2$d).ForEach(\n"
            + "function(v){\n"
            + "    g.V(v.id).Out(\"to\").ForEach(\n"
            + "      function(t){\n"
            + "        var node = {\n"
            + "          subject: g.V(v.id).Out(\"name\").ToValue(),\n"
            + "          subject_type: g.V(v.id).Out(\"type\").ToValue(),\n"
            + "          subject_namespace: g.V(v.id).Out(\"namespace\").ToValue(),\n"
            + "          predicate: \"to\",\n"
            + "          object: g.V(t.id).Out(\"name\").ToValue(),\n"
            + "          object_type: g.V(t.id).Out(\"type\").ToValue(),\n"
            + "          object_namespace: g.V(t.id).Out(\"namespace\").ToValue(),\n"
            + "        }\n"
            + "        g.Emit(node)\n"
            + "      }\n"
            + "    )\n"
            + "  }\n"
            + ")\n"
            + "g.V(\"%1$s\").FollowRecursive(g.M().In(\"to\"), %2$d).ForEach(\n"
            + "function(v){\n"
            + "    g.V(v.id).In(\"to\").ForEach(\n"
            + "      function(t){\n"
            + "        var node = {\n"
            + "          object: g.V(v.id).Out(\"name\").ToValue(),\n"
            + "          object_type: g.V(v.id).Out(\"type\").ToValue(),\n"
            + "          object_namespace: g.V(v.id).Out(\"namespace\").ToValue(),\n"
            + "          predicate: \"to\",\n"
            + "          subject: g.V(t.id).Out(\"name\").ToValue(),\n"
            + "          subject_type: g.V(t.id).Out(\"type\").ToValue(),\n"
            + "          subject_namespace: g.V(t.id).Out(\"namespace\").ToValue(),\n"
            + "        }\n"
            + "        g.Emit(node)\n"
            + "      }\n"
            + "    )\n"
            + "  }\n"
            + ")";

    return String.format(query, nodeName, depth);
  }

  private String doGizmoQuery(String query) throws MalformedURLException, IOException {
    log.info("running subgraph query" + query);

    // build and send request
    URL url = new URL(String.format("http://%s:%d/%s", host, port, GIZMO_QUERY_PATH));
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setDoOutput(true);
    con.setRequestMethod("POST");
    con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    BufferedWriter bodyWriter = new BufferedWriter(new OutputStreamWriter(con.getOutputStream()));
    bodyWriter.write(query);
    bodyWriter.flush();
    bodyWriter.close();

    // return string result
    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer content = new StringBuffer();
    while ((inputLine = in.readLine()) != null) {
      content.append(inputLine);
    }
    in.close();
    return content.toString();
  }

  public void linkJobToDataset(
      Job job, DatasetRowExtended dataset, String namespaceJob, String namespaceDataset)
      throws IOException {
    String jobSubject = buildNodeId(namespaceJob, job.getGuid().toString(), "job");
    String datasetObject = buildNodeId(namespaceDataset, dataset.getUuid().toString(), "dataset");

    CayleyTriple jobToDataset = new CayleyTriple(jobSubject, TO_PREDICATE, datasetObject);
    CayleyTriple jobToType = new CayleyTriple(jobSubject, TYPE_PREDICATE, JOB_TYPE);
    CayleyTriple datasetToType = new CayleyTriple(datasetObject, TYPE_PREDICATE, DATASET_TYPE);
    CayleyTriple jobToNamespace = new CayleyTriple(jobSubject, NAMESPACE_PREDICATE, namespaceJob);
    CayleyTriple datasetToNamespace =
        new CayleyTriple(datasetObject, NAMESPACE_PREDICATE, namespaceDataset);
    CayleyTriple jobToName = new CayleyTriple(jobSubject, NAME_PREDICATE, job.getName());
    CayleyTriple datasetToName = new CayleyTriple(datasetObject, NAME_PREDICATE, dataset.getName());

    this.write(jobToDataset);
    this.write(jobToType);
    this.write(datasetToType);
    this.write(datasetToNamespace);
    this.write(jobToNamespace);
    this.write(datasetToName);
    this.write(jobToName);
  }

  public void linkDatasetToJob(
      DatasetRowExtended dataset, Job job, String namespaceJob, String namespaceDataset)
      throws IOException {
    String datasetSubject = buildNodeId(namespaceDataset, dataset.getUuid().toString(), "dataset");
    String jobObject = buildNodeId(namespaceJob, job.getGuid().toString(), "job");

    CayleyTriple datasetToJob = new CayleyTriple(datasetSubject, TO_PREDICATE, jobObject);
    CayleyTriple datasetToType = new CayleyTriple(datasetSubject, TYPE_PREDICATE, DATASET_TYPE);
    CayleyTriple jobToType = new CayleyTriple(jobObject, TYPE_PREDICATE, JOB_TYPE);
    CayleyTriple jobToNamespace = new CayleyTriple(jobObject, NAMESPACE_PREDICATE, namespaceJob);
    CayleyTriple datasetToNamespace =
        new CayleyTriple(datasetSubject, NAMESPACE_PREDICATE, namespaceDataset);
    CayleyTriple jobToName = new CayleyTriple(jobObject, NAME_PREDICATE, job.getName());
    CayleyTriple datasetToName =
        new CayleyTriple(datasetSubject, NAME_PREDICATE, dataset.getName());

    this.write(datasetToJob);
    this.write(datasetToType);
    this.write(jobToType);
    this.write(datasetToNamespace);
    this.write(jobToNamespace);
    this.write(datasetToName);
    this.write(jobToName);
  }

  private String write(CayleyTriple triple) throws MalformedURLException, IOException {
    log.info("attempting to insert triple: " + triple.toString());
    // build and send request
    URL url = new URL(String.format("http://%s:%d/%s", host, port, QUAD_WRITE_PATH));
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setDoOutput(true);
    con.setRequestMethod("POST");
    con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    BufferedWriter bodyWriter = new BufferedWriter(new OutputStreamWriter(con.getOutputStream()));
    bodyWriter.write(String.format("[%s]", this.tripleWriter.writeValueAsString(triple)));
    bodyWriter.flush();
    bodyWriter.close();

    // return string result
    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer content = new StringBuffer();
    while ((inputLine = in.readLine()) != null) {
      content.append(inputLine);
    }
    in.close();
    return content.toString();
  }
}
