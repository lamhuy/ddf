/**
 * Copyright (c) Codice Foundation
 *
 * <p>This is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or any later version.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details. A copy of the GNU Lesser General Public
 * License is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 */
package ddf.catalog.provider.solr;

import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.IndexQueryResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.Request;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IndexProvider;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import ddf.catalog.util.impl.DescribableImpl;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang.Validate;
import org.codice.solr.factory.SolrClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class SolrIndexProvider extends DescribableImpl implements IndexProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexProvider.class);

  protected static final String DEFAULT_INDEX_CORE = "catalog_index";

  protected List<String> parameters;

  protected Map<String, String> tagToCore = new HashMap<>();

  protected Map<String, BaseSolrCatalogProvider> catalogProviders = new HashMap<>();

  protected final SolrClientFactory clientFactory;
  protected final FilterAdapter filterAdapter;
  protected final SolrFilterDelegateFactory solrFilterDelegateFactory;
  protected final DynamicSchemaResolver resolver;

  /**
   * Constructor that creates a new instance and allows for a custom {@link DynamicSchemaResolver}
   *
   * @param clientFactory Solr client factory
   * @param adapter injected implementation of FilterAdapter
   * @param resolver Solr schema resolver
   */
  public SolrIndexProvider(
      SolrClientFactory clientFactory,
      FilterAdapter adapter,
      SolrFilterDelegateFactory solrFilterDelegateFactory,
      DynamicSchemaResolver resolver) {
    Validate.notNull(clientFactory, "SolrClientFactory cannot be null.");
    Validate.notNull(adapter, "FilterAdapter cannot be null");
    Validate.notNull(solrFilterDelegateFactory, "SolrFilterDelegateFactory cannot be null");
    Validate.notNull(resolver, "DynamicSchemaResolver cannot be null");

    this.clientFactory = clientFactory;
    this.filterAdapter = adapter;
    this.solrFilterDelegateFactory = solrFilterDelegateFactory;
    this.resolver = resolver;

    // Always add default provider. Solr Cloud this will be an aggregate Alias,
    // and standalone it will be the only index core
    LOGGER.debug("Adding provider for core: {}", DEFAULT_INDEX_CORE);
    catalogProviders.computeIfAbsent(DEFAULT_INDEX_CORE, this::newProvider);
  }

  /**
   * Convenience constructor that creates a new ddf.catalog.source.solr.DynamicSchemaResolver
   *
   * @param clientFactory Solr client factory
   * @param adapter injected implementation of FilterAdapter
   */
  public SolrIndexProvider(
      SolrClientFactory clientFactory,
      FilterAdapter adapter,
      SolrFilterDelegateFactory solrFilterDelegateFactory) {
    this(clientFactory, adapter, solrFilterDelegateFactory, new DynamicSchemaResolver());
  }

  protected BaseSolrCatalogProvider newProvider(String core) {
    return new BaseSolrCatalogProvider(
        clientFactory.getClient(core), filterAdapter, solrFilterDelegateFactory, resolver);
  }

  @Override
  public CreateResponse create(CreateRequest createRequest) throws IngestException {
    LOGGER.trace("Create request received");
    return getCatalogProvider(createRequest).create(createRequest);
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    LOGGER.trace("Update request received");
    return getCatalogProvider(updateRequest).update(updateRequest);
  }

  @Override
  public void delete(DeleteRequest deleteRequest) throws IngestException {
    /** Delete across all collections Assuming id uniqueness is preserved across all collection */
    for (String core : catalogProviders.keySet()) {
      catalogProviders.get(core).deleteIndex(deleteRequest);
      LOGGER.debug(
          "Deleting {} items from core: {}", deleteRequest.getAttributeValues().size(), core);
    }
  }

  @Override
  public IndexQueryResponse query(QueryRequest queryRequest) throws UnsupportedQueryException {
    return getCatalogProvider(queryRequest).queryIndex(queryRequest);
  }

  public void shutdown() {
    LOGGER.debug("Closing down Solr client.");
    this.catalogProviders.forEach((k, p) -> p.shutdown());
  }

  @Override
  public void maskId(String id) {
    catalogProviders.forEach((k, p) -> p.maskId(id));
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(List<String> parameters) {
    this.parameters = parameters;

    if (clientFactory.isSolrCloud()) {
      for (String parameter : parameters) {
        String[] tagCorePair = parameter.split("=");
        String core = tagCorePair[1];
        tagToCore.put(tagCorePair[0], core);
        LOGGER.debug("Adding provider for core: {}", core);
        catalogProviders.computeIfAbsent(core, this::newProvider);
      }
    }
  }

  /** TODO: assuming all metacards are of the same type /tag for each request * */
  protected BaseSolrCatalogProvider getCatalogProvider(Request request) {
    if (!clientFactory.isSolrCloud()) {
      LOGGER.trace("Non SolrCloud instance using index core: {}", DEFAULT_INDEX_CORE);
      return catalogProviders.get(DEFAULT_INDEX_CORE);
    }

    Set<String> tags = new HashSet<>();
    if (request instanceof CreateRequest) {
      tags.addAll(
          ((CreateRequest) request)
              .getMetacards()
              .stream()
              .findFirst()
              .map(c -> c.getTags())
              .get());

    } else if (request instanceof UpdateRequest) {
      tags.addAll(
          ((UpdateRequest) request)
              .getUpdates()
              .stream()
              .findFirst()
              .map(m -> m.getValue())
              .map(c -> c.getTags())
              .get());
    } else if (request instanceof QueryRequest) {
      LOGGER.trace("Query requests using core: {}", DEFAULT_INDEX_CORE);
      return catalogProviders.get(DEFAULT_INDEX_CORE);
    }

    String core = getCore(tags);
    LOGGER.warn("Using {} core for the request", core);
    return catalogProviders.get(core);
  }

  /**
   * From a list of tags extracted from a metacard, return the associate solr core assuming ...
   *
   * @param tags all tags from a metacard
   * @return the first associate solr core for those given tags
   */
  private String getCore(Set<String> tags) {
    LOGGER.trace("Getting core for tags: {}", tags);
    return tags.stream()
        .map(t -> tagToCore.get(t))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(DEFAULT_INDEX_CORE);
    // return tagToCore.getOrDefault(tags, DEFAULT_INDEX_CORE);
  }
}
