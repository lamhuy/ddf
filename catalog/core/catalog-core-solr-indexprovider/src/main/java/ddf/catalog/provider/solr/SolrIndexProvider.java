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

import ddf.catalog.data.Metacard;
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
import ddf.catalog.source.solr.api.IndexCollectionProvider;
import ddf.catalog.source.solr.api.SolrCollectionConfiguration;
import ddf.catalog.source.solr.api.SolrCollectionCreationPlugin;
import ddf.catalog.util.impl.DescribableImpl;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.codice.solr.factory.SolrClientFactory;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class SolrIndexProvider extends DescribableImpl implements IndexProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexProvider.class);

  protected static final String DEFAULT_INDEX_CORE = "catalog_index";

  protected final Map<String, BaseSolrCatalogProvider> catalogProviders = new ConcurrentHashMap<>();

  protected final SolrClientFactory clientFactory;

  protected final FilterAdapter filterAdapter;

  protected final SolrFilterDelegateFactory solrFilterDelegateFactory;

  protected final DynamicSchemaResolver resolver;

  protected final List<IndexCollectionProvider> indexCollectionProviders;

  protected final List<SolrCollectionCreationPlugin> collectionCreationPlugins;

  protected static final Set<String> COLLECTIONS = new ConcurrentHashSet<>();

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
      DynamicSchemaResolver resolver,
      List<IndexCollectionProvider> indexCollectionProviders,
      List<SolrCollectionCreationPlugin> collectionCreationPlugins) {
    Validate.notNull(clientFactory, "SolrClientFactory cannot be null.");
    Validate.notNull(adapter, "FilterAdapter cannot be null");
    Validate.notNull(solrFilterDelegateFactory, "SolrFilterDelegateFactory cannot be null");
    Validate.notNull(resolver, "DynamicSchemaResolver cannot be null");
    Validate.notEmpty(indexCollectionProviders, "IndexCollectionProviders cannot be empty");
    Validate.notNull(collectionCreationPlugins, "SolrCollectionCreationPlugin list must exist");

    this.clientFactory = clientFactory;
    this.filterAdapter = adapter;
    this.solrFilterDelegateFactory = solrFilterDelegateFactory;
    this.resolver = resolver;
    this.indexCollectionProviders = indexCollectionProviders;
    this.collectionCreationPlugins = collectionCreationPlugins;

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
      SolrFilterDelegateFactory solrFilterDelegateFactory,
      List<IndexCollectionProvider> indexCollectionProviders,
      List<SolrCollectionCreationPlugin> collectionCreationPlugins) {
    this(
        clientFactory,
        adapter,
        solrFilterDelegateFactory,
        new DynamicSchemaResolver(),
        indexCollectionProviders,
        collectionCreationPlugins);
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
              .map(Metacard::getTags)
              .get());

    } else if (request instanceof UpdateRequest) {
      tags.addAll(
          ((UpdateRequest) request)
              .getUpdates()
              .stream()
              .findFirst()
              .map(Map.Entry::getValue)
              .map(Metacard::getTags)
              .get());
    } else if (request instanceof QueryRequest) {
      LOGGER.trace("Query requests using core: {}", DEFAULT_INDEX_CORE);
      return catalogProviders.get(DEFAULT_INDEX_CORE);
    }

    //    LOGGER.warn("Using {} core for the request", core);
    //    return catalogProviders.get(core);
    // TODO TROY -- update this to keep a map of collection + providers
    return null;
  }

  /**
   * From a list of tags extracted from a metacard, return the associate solr core assuming ...
   *
   * @param metacard The metacard used to determine collection
   * @return the first associate solr core for those given tags
   */
  private String getCore(Metacard metacard) {
    LOGGER.trace("Getting core for metacard: {}", metacard);

    for (IndexCollectionProvider provider : indexCollectionProviders) {
      String collection = provider.getCollection(metacard);
      if (StringUtils.isNotBlank(collection)) {
        createCollectionIfRequired(collection, provider);
        return collection;
      }
    }
    return DEFAULT_INDEX_CORE;
  }

  private void createCollectionIfRequired(String collection, IndexCollectionProvider provider) {
    if (!collectionExists(collection)) {
      createCollection(collection, provider);
      for (SolrCollectionCreationPlugin plugin : collectionCreationPlugins) {
        plugin.collectionCreated(collection);
      }
    }
  }

  private boolean collectionExists(String collection) {
    if (COLLECTIONS.contains(collection)) {
      return true;
    }

    synchronized (COLLECTIONS) {
      if (clientFactory.collectionExists(collection)) {
        COLLECTIONS.add(collection);
        return true;
      } else {
        return false;
      }
    }
  }

  private void createCollection(String collection, IndexCollectionProvider provider) {
    SolrCollectionConfiguration configuration = provider.getConfiguration();
    clientFactory.addConfiguration(
        configuration.getConfigurationName(), configuration.getSolrConfigurationData());
    clientFactory.addCollection(
        collection, configuration.getDefaultNumShards(), configuration.getConfigurationName());
  }
}
