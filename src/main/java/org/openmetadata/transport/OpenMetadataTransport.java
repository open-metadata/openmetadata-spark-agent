/*
 *  Copyright 2024 Collate
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * This code has been referenced from
 * https://github.com/Natural-Intelligence/openLineage-openMetadata-transporter.git
 */

package org.openmetadata.transport;

import static org.apache.http.Consts.UTF_8;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.Transport;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

@Slf4j
public final class OpenMetadataTransport extends Transport implements Closeable {

  private static final String SPARK_LINEAGE_SOURCE = "SparkLineage";
  private static final String TABLE_SEARCH_INDEX = "table_search_index";
  private static final String CONTAINER_SEARCH_INDEX = "container_search_index";

  private static final String PIPELINE_SOURCE_TYPE = "Spark";

  private final CloseableHttpClient http;
  private final URI uri;
  private final String pipelineServiceName;

  private List<String> databaseServiceNames;

  private final String pipelineName;
  private final String authToken;
  private @Nullable final String pipelineSourceUrl;
  private @Nullable final String pipelineDescription;

  public OpenMetadataTransport(@NonNull final OpenMetadataConfig openMetadataConfig) {
    this(withTimeout(openMetadataConfig.getTimeout()), openMetadataConfig);
  }

  public OpenMetadataTransport(
      @NonNull final CloseableHttpClient httpClient,
      @NonNull final OpenMetadataConfig openMetadataConfig) {
    this.http = httpClient;
    this.uri = openMetadataConfig.getHostPort();
    this.authToken = openMetadataConfig.getJwtToken();
    this.pipelineName = openMetadataConfig.getPipelineName();
    this.pipelineServiceName = openMetadataConfig.getPipelineServiceName();
    this.pipelineSourceUrl = openMetadataConfig.getPipelineSourceUrl();
    this.pipelineDescription = openMetadataConfig.getPipelineDescription();
    String dbServiceNameStr = openMetadataConfig.getDatabaseServiceNames();
    if (dbServiceNameStr != null) {
      try {
        this.databaseServiceNames = Arrays.asList(dbServiceNameStr.split(","));
      } catch (Exception e) {
        log.error("failed to emit fetch database service names: {}", e.getMessage(), e);
      }
    } else {
      this.databaseServiceNames = new ArrayList<>();
    }
    createOrUpdatePipelineService();
  }

  private static CloseableHttpClient withTimeout(Double timeout) {
    int timeoutMs;
    if (timeout == null) {
      timeoutMs = 5000;
    } else {
      timeoutMs = (int) (timeout * 1000);
    }

    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(timeoutMs)
            .setConnectionRequestTimeout(timeoutMs)
            .setSocketTimeout(timeoutMs)
            .build();
    return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    try {
      log.debug(
          "Captured Event Type " + runEvent.getEventType().toString() + " to Parse for Lineage");
      for (OpenLineage.InputDataset input : runEvent.getInputs()) {
        log.debug("Input Dataset: " + input.getName());
      }
      for (OpenLineage.OutputDataset output : runEvent.getOutputs()) {
        log.debug("Output Dataset: " + output.getName());
      }
      if (((runEvent.getEventType().equals(OpenLineage.RunEvent.EventType.COMPLETE))
              || (runEvent.getEventType().equals(OpenLineage.RunEvent.EventType.START)))
          && !runEvent.getInputs().isEmpty()
          && !runEvent.getOutputs().isEmpty()) {
        sendToOpenMetadata(runEvent.getInputs(), runEvent.getOutputs());
      }
    } catch (Exception e) {
      log.error("failed to emit event to OpenMetadata: {}", e.getMessage(), e);
    }
  }

  private String getTableNames(OpenLineage.Dataset dataset) {
    if (dataset == null) {
      return null;
    }
    String tableName = extractTableNamesFromSymlinks(dataset);

    // Handle table names from JDBC queries that don't have symlinks
    if (tableName == null) {
      tableName = extractTableNamesFromDataSet(dataset);
    }

    return tableName;
  }

  private boolean isContainer(String name) {
    return name.startsWith("s3a://") || name.startsWith("s3://");
  }

  private String extractTableNamesFromSymlinks(OpenLineage.Dataset dataset) {
    if (dataset.getFacets() != null
        && dataset.getFacets().getSymlinks() != null
        && dataset.getFacets().getSymlinks().getIdentifiers() != null) {
      for (OpenLineage.SymlinksDatasetFacetIdentifiers identifier :
          dataset.getFacets().getSymlinks().getIdentifiers()) {
        if (isContainer(identifier.getNamespace())) {
          return identifier.getNamespace().replace("s3a://", "s3://") + identifier.getName();
        }
        return identifier.getName();
      }
    }
    return null;
  }

  private String extractTableNamesFromDataSet(OpenLineage.Dataset dataset) {
    if (dataset != null && dataset.getName() != null && dataset.getNamespace() != null) {
      return generateTableName(dataset.getName(), dataset.getNamespace());
    }
    return null;
  }

  private String generateTableName(String name, String namespace) {
    if (isContainer(namespace)) {
      return namespace.replace("s3a://", "s3://") + name;
    }
    if (!name.contains(".")) {
      String dbName = extractDbNameFromUrl(namespace);
      if (dbName != null) {
        return dbName + "." + name;
      }
    }
    return name;
  }

  public void sendToOpenMetadata(
      List<? extends OpenLineage.Dataset> inputTables,
      List<? extends OpenLineage.Dataset> outputTables) {
    String pipelineId = createOrUpdatePipeline();
    for (OpenLineage.Dataset fromTable : inputTables) {
      String inputTableName = getTableNames(fromTable);
      if (inputTableName == null) {
        continue;
      }
      Map<String, Object> fromEntity = getEntity(inputTableName);
      for (OpenLineage.Dataset toTable : outputTables) {
        String outputTableName = getTableNames(toTable);
        if (outputTableName == null) {
          continue;
        }
        Map<String, Object> toEntity = getEntity(outputTableName);
        createOrUpdateLineage(pipelineId, fromEntity, toEntity, fromTable, toTable);
        log.info(
            String.format(
                "lineage was sent successfully to OpenMetadata for fromTable: %s, toTable: %s",
                inputTableName, outputTableName));
      }
    }
  }

  private Map<String, Object> getEntity(String tableName, String dbServiceName) {
    try {
      HttpGet request = createGetRequest(tableName, dbServiceName);
      Map<String, Object> response = sendRequest(request);
      Map<String, Object> hitsResult = (Map<String, Object>) response.get("hits");
      int totalHits =
          Integer.parseInt(((Map<String, Object>) hitsResult.get("total")).get("value").toString());
      if (totalHits == 0) {
        log.debug("Failed to get id of table {} from OpenMetadata.", tableName);
        return new HashMap<>();
      }
      List<Map<String, Object>> tablesData = (List<Map<String, Object>>) hitsResult.get("hits");
      return tablesData.stream()
          .map(t -> ((Map<String, Object>) t.get("_source")))
          .collect(Collectors.toList())
          .get(0);

    } catch (Exception e) {
      log.error("Failed to get table entity {} from OpenMetadata: ", tableName, e);
      throw new OpenLineageClientException(e);
    }
  }

  private Map<String, Object> getEntity(String tableName) {
    if (this.databaseServiceNames == null || this.databaseServiceNames.isEmpty()) {
      return getEntity(tableName, null);
    }
    for (String dbService : this.databaseServiceNames) {
      Map<String, Object> result = getEntity(tableName, dbService);
      if (result == null) {
        continue;
      }
      return result;
    }
    return new HashMap<>();
  }

  public HttpGet createESRequest(String fieldName, String fieldValue, String index)
      throws Exception {
    String path = "api/v1/search/fieldQuery";
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("size", "10");
    queryParams.put("fieldName", fieldName);
    queryParams.put("fieldValue", fieldValue);
    queryParams.put("from", "0");
    queryParams.put("index", index);
    return createGetRequest(path, queryParams);
  }

  public HttpGet createGetContainerRequest(String containerPath) throws Exception {
    return createESRequest("fullPath", containerPath, CONTAINER_SEARCH_INDEX);
  }

  public HttpGet createGetTableRequest(String tableName, String dbServiceName) throws Exception {
    String fqnQuery;
    if (dbServiceName != null) {
      fqnQuery = dbServiceName + ".*" + tableName;
    } else {
      fqnQuery = "*" + tableName;
    }
    return createESRequest("fullyQualifiedName", fqnQuery, TABLE_SEARCH_INDEX);
  }

  public HttpGet createGetRequest(String tableName, String dbServiceName) throws Exception {
    if (isContainer(tableName)) {
      return createGetContainerRequest(tableName);
    }
    return createGetTableRequest(tableName, dbServiceName);
  }

  private String createOrUpdatePipelineService() {
    try {
      HttpPut request = createPipelineServiceRequest();
      Map response = sendRequest(request);
      return response.get("id").toString();
    } catch (Exception e) {
      log.error(
          "Failed to create/update service pipeline {} in OpenMetadata: ", pipelineServiceName, e);
      throw new OpenLineageClientException(e);
    }
  }

  private String createOrUpdatePipeline() {
    try {
      HttpPut request = createPipelineRequest();
      Map<String, Object> response = sendRequest(request);
      return response.get("id").toString();
    } catch (Exception e) {
      log.error("Failed to create/update pipeline {} in OpenMetadata: ", pipelineName, e);
      throw new OpenLineageClientException(e);
    }
  }

  private void createOrUpdateLineage(
      String pipelineId,
      Map<String, Object> fromEntity,
      Map<String, Object> toEntity,
      OpenLineage.Dataset fromTable,
      OpenLineage.Dataset toTable) {
    try {
      if (fromEntity != null
          && fromEntity.get("id") != null
          && toEntity != null
          && toEntity.get("id") != null) {
        HttpPut request =
            createLineageRequest(pipelineId, fromEntity, toEntity, fromTable, toTable);
        sendRequest(request);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new OpenLineageClientException(e);
    }
  }

  private Map<String, Object> sendRequest(HttpRequestBase request) throws IOException {
    try (CloseableHttpResponse response = http.execute(request)) {
      throwOnHttpError(response);
      String jsonResponse = EntityUtils.toString(response.getEntity());
      return fromJsonString(jsonResponse);
    }
  }

  private String getColumnFQN(Map tableEntity, String columnName) {
    return tableEntity.get("fullyQualifiedName") + "." + columnName;
  }

  private Set<Map<String, Object>> getColumnLevelLineage(
      Map<String, Object> fromTableEntity,
      Map<String, Object> toTableEntity,
      OpenLineage.Dataset fromTable,
      OpenLineage.Dataset toTable) {
    Set<Map<String, Object>> columnLineage = new HashSet<>();
    try {
      if (toTable.getFacets() != null
          && toTable.getFacets().getColumnLineage() != null
          && toTable.getFacets().getColumnLineage().getFields() != null
          && toTable.getFacets().getColumnLineage().getFields().getAdditionalProperties() != null) {
        Set<String> fromColSet = new HashSet((Collection) fromTableEntity.get("columnNames"));
        Set<String> toColSet = new HashSet((Collection) toTableEntity.get("columnNames"));
        toTable
            .getFacets()
            .getColumnLineage()
            .getFields()
            .getAdditionalProperties()
            .forEach(
                (key, value) -> {
                  if (toColSet.contains(key)) {
                    value
                        .getInputFields()
                        .forEach(
                            input -> {
                              if (input.getName().equals(fromTable.getName())
                                  && fromColSet.contains(input.getField())) {
                                Map<String, Object> columnLineageMap = new HashMap<>();
                                columnLineageMap.put(
                                    "fromColumns",
                                    new String[] {getColumnFQN(fromTableEntity, input.getField())});
                                columnLineageMap.put("toColumn", getColumnFQN(toTableEntity, key));
                                columnLineage.add(columnLineageMap);
                              }
                            });
                  }
                });
      }

    } catch (Exception err) {
      log.error(
          String.format("Failed to extract column level lineage due to %s", err.getMessage()));
      err.printStackTrace();
    }

    return columnLineage;
  }

  public HttpPut createLineageRequest(
      String pipelineId,
      Map<String, Object> fromEntity,
      Map<String, Object> toEntity,
      OpenLineage.Dataset fromTable,
      OpenLineage.Dataset toTable)
      throws JsonProcessingException {
    Map<String, Object> edgeMap = new HashMap<>();
    edgeMap.put(
        "toEntity",
        createEntityMap(toEntity.get("entityType").toString(), toEntity.get("id").toString()));
    edgeMap.put(
        "fromEntity",
        createEntityMap(fromEntity.get("entityType").toString(), fromEntity.get("id").toString()));
    Map<String, Object> lineageDetails = new HashMap<>();
    lineageDetails.put("pipeline", createEntityMap("pipeline", pipelineId));
    lineageDetails.put("source", SPARK_LINEAGE_SOURCE);
    lineageDetails.put(
        "columnsLineage", getColumnLevelLineage(fromEntity, toEntity, fromTable, toTable));
    edgeMap.put("lineageDetails", lineageDetails);
    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put("edge", edgeMap);
    String jsonRequest = toJsonString(requestMap);
    return createPutRequest("/api/v1/lineage", jsonRequest);
  }

  private String toJsonString(Map<String, Object> map) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(map);
  }

  private Map<String, Object> fromJsonString(String jsonString) throws JsonProcessingException {
    if (jsonString == null || jsonString.isEmpty()) {
      return new HashMap<>();
    }
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(jsonString, Map.class);
  }

  private Map<String, Object> createEntityMap(String type, String id) {
    Map<String, Object> entityMap = new HashMap<>();
    entityMap.put("type", type);
    entityMap.put("id", id);
    return entityMap;
  }

  public HttpGet createGetRequest(String path, Map<String, String> queryParams) throws Exception {
    return (HttpGet) createHttpRequest(HttpGet::new, path, queryParams);
  }

  public HttpPut createPutRequest(String path, String jsonRequest) {
    try {
      HttpPut request = (HttpPut) createHttpRequest(HttpPut::new, path, null);
      request.setEntity(new StringEntity(jsonRequest, APPLICATION_JSON));
      return request;
    } catch (Exception exception) {
      log.error(
          String.format(
              "Failed to create lineage in OpenMetadata due to %s", exception.getMessage()));
      exception.printStackTrace();
    }
    return null;
  }

  private HttpRequestBase createHttpRequest(
      Supplier<HttpRequestBase> supplier, String path, Map<String, String> queryParams)
      throws URISyntaxException {
    URIBuilder uriBuilder = new URIBuilder(this.uri);
    uriBuilder.setPath(path);
    if (queryParams != null) {
      queryParams.entrySet().forEach(e -> uriBuilder.addParameter(e.getKey(), e.getValue()));
    }
    URI omUri = uriBuilder.build();
    final HttpRequestBase request = supplier.get();
    request.setURI(omUri);
    request.addHeader(ACCEPT, APPLICATION_JSON.toString());
    request.addHeader(CONTENT_TYPE, APPLICATION_JSON.toString());

    if (authToken != null) {
      request.addHeader(AUTHORIZATION, authToken);
    }
    return request;
  }

  public HttpPut createPipelineServiceRequest() throws JsonProcessingException {
    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put("name", pipelineServiceName);
    requestMap.put("serviceType", PIPELINE_SOURCE_TYPE);

    Map<String, Object> connectionConfig = new HashMap<>();
    HashMap<String, String> connectionType = new HashMap<>();
    connectionType.put("type", PIPELINE_SOURCE_TYPE);
    connectionConfig.put("config", connectionType);
    requestMap.put("connection", connectionConfig);
    String jsonRequest = toJsonString(requestMap);
    return createPutRequest("/api/v1/services/pipelineServices", jsonRequest);
  }

  public HttpPut createPipelineRequest() throws JsonProcessingException {
    Map<String, Object> requestMap = new HashMap<>();
    requestMap.put("name", pipelineName);
    requestMap.put("sourceUrl", pipelineSourceUrl);

    if (pipelineDescription != null && !pipelineDescription.isEmpty()) {
      requestMap.put("description", pipelineDescription);
    }
    requestMap.put("service", pipelineServiceName);
    String jsonRequest = toJsonString(requestMap);
    return createPutRequest("/api/v1/pipelines", jsonRequest);
  }

  public String extractDbNameFromUrl(String url) {
    if (url != null) {
      Pattern pattern = Pattern.compile("^[^:]+://[^/]+:[0-9]+/([^?]+)");
      Matcher matcher = pattern.matcher(url);

      if (matcher.find()) {
        return matcher.group(1);
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    http.close();
  }

  private void throwOnHttpError(@NonNull HttpResponse response) throws IOException {
    final int code = response.getStatusLine().getStatusCode();
    if (code >= 400 && code < 600) { // non-2xx
      String message =
          String.format(
              "code: %d, response: %s", code, EntityUtils.toString(response.getEntity(), UTF_8));
      throw new OpenLineageClientException(message);
    }
  }
}
