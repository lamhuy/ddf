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
import ddf.catalog.operation.ProcessingDetails;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.Request;
import ddf.catalog.operation.Response;
import ddf.catalog.operation.Update;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.CreateResponseImpl;
import ddf.catalog.operation.impl.IndexQueryResponseImpl;
import ddf.catalog.operation.impl.UpdateRequestImpl;
import ddf.catalog.operation.impl.UpdateResponseImpl;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IndexProvider;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.ConfigurationStore;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.RealTimeGetDelegate;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import ddf.catalog.source.solr.api.IndexCollectionProvider;
import ddf.catalog.source.solr.api.SolrCollectionCreationPlugin;
import ddf.catalog.util.impl.MaskableImpl;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.math.NumberUtils;
import org.codice.ddf.platform.util.StandardThreadFactoryBuilder;
import org.codice.solr.factory.SolrClientFactory;
import org.codice.solr.factory.SolrCollectionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class SolrIndexProvider extends MaskableImpl implements IndexProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexProvider.class);

  protected static final String DEFAULT_INDEX_CORE = "catalog_index";

  protected static final String QUERY_ALIAS = "catalog";

  protected static final String CATALOG_PREFIX = "catalog_";

  private static final String QUERY_POOL_NAME = "solr-indexprovider-query-pool";

  private static final int MAX_Q_SIZE = 128;

  protected final Map<String, BaseSolrCatalogProvider> catalogProviders = new ConcurrentHashMap<>();

  protected final SolrClientFactory clientFactory;

  protected final FilterAdapter filterAdapter;

  protected final SolrFilterDelegateFactory solrFilterDelegateFactory;

  protected final DynamicSchemaResolver resolver;

  protected final List<IndexCollectionProvider> indexCollectionProviders;

  protected final List<SolrCollectionCreationPlugin> collectionCreationPlugins;

  private ThreadPoolExecutor queryExecutor;

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

    initThreads();

    // Always add default provider. Solr Cloud this will be an aggregate Alias,
    // and standalone it will be the only index core
    if (!clientFactory.isSolrCloud()) {
      LOGGER.debug("Adding provider for core: {}", DEFAULT_INDEX_CORE);
      catalogProviders.computeIfAbsent(DEFAULT_INDEX_CORE, this::newProvider);
    } else {
      ensureDefaultCollectionExists();
      initializeKnownProviders();
    }
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
    if (clientFactory.isSolrCloud() && core.equals(QUERY_ALIAS)) {
      ensureAliasExists(QUERY_ALIAS, CATALOG_PREFIX);
    }
    return new BaseSolrCatalogProvider(
        clientFactory.newClient(core), filterAdapter, solrFilterDelegateFactory, resolver);
  }

  @Override
  public CreateResponse create(CreateRequest createRequest) throws IngestException {
    LOGGER.trace("Create request received");
    return (CreateResponse) executeRequest(createRequest);
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    LOGGER.trace("Update request received");
    return (UpdateResponse) executeRequest(updateRequest);
  }

  @Override
  public void delete(DeleteRequest deleteRequest) throws IngestException {
    /** Delete across all collections Assuming id uniqueness is preserved across all collection */
    int numItems = deleteRequest.getAttributeValues().size();
    for (Map.Entry<String, BaseSolrCatalogProvider> entry : catalogProviders.entrySet()) {
      entry.getValue().deleteIndex(deleteRequest);
      LOGGER.debug(
          "Sending delete request for {} items to collection: {}", numItems, entry.getKey());
    }
  }

  @Override
  public IndexQueryResponse query(QueryRequest queryRequest) throws UnsupportedQueryException {
    if (!clientFactory.isSolrCloud()) {
      return catalogProviders
          .computeIfAbsent(DEFAULT_INDEX_CORE, this::newProvider)
          .queryIndex(queryRequest);
    }

    Boolean doRealTimeGet = filterAdapter.adapt(queryRequest.getQuery(), new RealTimeGetDelegate());
    if (BooleanUtils.isTrue(doRealTimeGet)) {
      // Need to call get handler on every index core
      List<String> ids = new ArrayList<>();
      long totalHits = 0;

      for (BaseSolrCatalogProvider provider : getAliasProviderNonAlias()) {
        long providerStart = System.currentTimeMillis();

        IndexQueryResponse response = provider.queryIndexCache(queryRequest);
        ids.addAll(response.getIds());
        totalHits += response.getHits();

        if (LOGGER.isTraceEnabled()) {
          long total = System.currentTimeMillis() - providerStart;
          LOGGER.trace("Provider took {} ms to respond", total);
        }
      }
      return new IndexQueryResponseImpl(queryRequest, ids, totalHits);
    } else {
      if (catalogProviders.size() <= 2) {
        // Query against the common index core/alias
        ensureDefaultCollectionExists();
        return catalogProviders
            .computeIfAbsent(QUERY_ALIAS, this::newProvider)
            .queryIndex(queryRequest);
      }

      // TODO TROY -- implementation using manual parallel query
      LOGGER.debug("Running custom parallel query");

      List<Future<IndexQueryResponse>> futures = new ArrayList<>(catalogProviders.size());
      CompletionService<IndexQueryResponse> queryService =
          new ExecutorCompletionService<>(queryExecutor);
      for (BaseSolrCatalogProvider provider : getAliasProviderNonAlias()) {
        Callable queryCallable = () -> provider.queryIndex(queryRequest);
        futures.add(queryService.submit(queryCallable));
      }
      List<String> ids = new ArrayList<>();
      long totalHits = 0;

      for (Future<IndexQueryResponse> completedQuery : futures) {
        try {
          IndexQueryResponse response = completedQuery.get();
          totalHits += response.getHits();
          ids.addAll(response.getIds());
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.debug("Unable to get query response", e);
          Thread.currentThread().interrupt();
        }
      }
      return new IndexQueryResponseImpl(queryRequest, ids, totalHits);
    }
  }

  public void shutdown() {
    LOGGER.debug("Closing down Solr client.");
    this.catalogProviders.forEach((k, p) -> p.shutdown());
  }

  @Override
  public void maskId(String id) {
    catalogProviders.forEach((k, p) -> p.maskId(id));
  }

  @Override
  public boolean isAvailable() {
    if (clientFactory != null) {
      return clientFactory.isAvailable();
    }
    return false;
  }

  @Override
  public void setForceAutoCommit(boolean forceAutoCommit) {
    ConfigurationStore.getInstance().setForceAutoCommit(forceAutoCommit);
  }

  protected Response executeRequest(Request request) throws IngestException {
    if (!clientFactory.isSolrCloud()) {
      LOGGER.trace("Non SolrCloud instance using index core: {}", DEFAULT_INDEX_CORE);
      if (request instanceof CreateRequest) {
        return catalogProviders
            .computeIfAbsent(DEFAULT_INDEX_CORE, this::newProvider)
            .create((CreateRequest) request);
      } else if (request instanceof UpdateRequest) {
        return catalogProviders
            .computeIfAbsent(DEFAULT_INDEX_CORE, this::newProvider)
            .update((UpdateRequest) request);
      }
      return null;
    }

    if (request instanceof CreateRequest) {
      Map<String, Serializable> props = new HashMap<>();
      List<Metacard> createdMetacards = new ArrayList<>();
      Set<ProcessingDetails> errors = new HashSet<>();
      Map<String, CreateRequest> requests = getCreateRequests((CreateRequest) request);
      for (Map.Entry<String, CreateRequest> entry : requests.entrySet()) {
        CreateResponse response =
            catalogProviders
                .computeIfAbsent(entry.getKey(), this::newProvider)
                .create(entry.getValue());
        props.putAll(response.getProperties());
        createdMetacards.addAll(response.getCreatedMetacards());
        errors.addAll(response.getProcessingErrors());
      }

      return new CreateResponseImpl((CreateRequest) request, props, createdMetacards, errors);
    } else if (request instanceof UpdateRequest) {
      Map<String, UpdateRequest> requests = getUpdateRequests((UpdateRequest) request);
      Map<String, Serializable> props = new HashMap<>();
      List<Metacard> updatedMetacards = new ArrayList<>();
      List<Metacard> oldMetacards = new ArrayList<>();
      Set<ProcessingDetails> errors = new HashSet<>();
      for (Map.Entry<String, UpdateRequest> entry : requests.entrySet()) {
        UpdateResponse response =
            catalogProviders
                .computeIfAbsent(entry.getKey(), this::newProvider)
                .update(entry.getValue());
        if (response.getProperties() != null) {
          props.putAll(response.getProperties());
        }
        for (Update update : response.getUpdatedMetacards()) {
          updatedMetacards.add(update.getNewMetacard());
          oldMetacards.add(update.getOldMetacard());
        }
        errors.addAll(response.getProcessingErrors());
      }

      return new UpdateResponseImpl(
          (UpdateRequest) request, props, updatedMetacards, oldMetacards, errors);
    }

    return null;
  }

  /**
   * From a list of tags extracted from a metacard, return the associate solr core assuming ...
   *
   * @param metacard The metacard used to determine collection
   * @return the first associate solr core for those given tags
   */
  private String getCollection(Metacard metacard) {
    LOGGER.trace("Getting collection for metacard: {}", metacard);

    for (IndexCollectionProvider provider : indexCollectionProviders) {
      String collection = provider.getCollection(metacard);
      if (collection != null) {
        createCollectionIfRequired(collection, provider);
        return collection;
      }
    }
    return DEFAULT_INDEX_CORE;
  }

  private void createCollectionIfRequired(String collection, IndexCollectionProvider provider) {
    if (StringUtils.isBlank(collection)) {
      return;
    }

    if (!collectionExists(collection)) {
      createCollection(collection, provider);
      for (SolrCollectionCreationPlugin plugin : collectionCreationPlugins) {
        plugin.collectionCreated(collection);
      }
    }
  }

  private boolean collectionExists(String collection) {
    if (catalogProviders.containsKey(collection)) {
      return true;
    }

    synchronized (catalogProviders) {
      if (clientFactory.collectionExists(collection)) {
        catalogProviders.put(collection, newProvider(collection));
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
    waitForCollection(collection);
    clientFactory.addCollectionToAlias(QUERY_ALIAS, collection, CATALOG_PREFIX);
  }

  private void waitForCollection(final String collection) {
    RetryPolicy retryPolicy =
        new RetryPolicy()
            .withDelay(100, TimeUnit.MILLISECONDS)
            .withMaxDuration(3, TimeUnit.MINUTES)
            .retryWhen(false);
    Failsafe.with(retryPolicy).run(() -> clientFactory.collectionExists(collection));
  }

  private Map<String, CreateRequest> getCreateRequests(CreateRequest createRequest) {
    Map<String, CreateRequest> createRequests = new HashMap<>();
    Map<String, List<Metacard>> collectionMetacardMap = new HashMap<>();
    for (Metacard metacard : createRequest.getMetacards()) {
      String collection = getCollection(metacard);
      if (collectionMetacardMap.containsKey(collection)) {
        List<Metacard> metacards = collectionMetacardMap.get(collection);
        metacards.add(metacard);
      } else {
        List<Metacard> metacards = new ArrayList<>();
        metacards.add(metacard);
        collectionMetacardMap.put(collection, metacards);
      }
    }
    for (Map.Entry<String, List<Metacard>> entry : collectionMetacardMap.entrySet()) {
      CreateRequest request =
          new CreateRequestImpl(
              entry.getValue(), createRequest.getProperties(), createRequest.getStoreIds());
      createRequests.put(entry.getKey(), request);
    }

    return createRequests;
  }

  private Map<String, UpdateRequest> getUpdateRequests(UpdateRequest updateRequest) {
    Map<String, UpdateRequest> updateRequests = new HashMap<>();
    Map<String, List<Entry<Serializable, Metacard>>> collectionMetacardMap = new HashMap<>();
    for (Entry<Serializable, Metacard> entry : updateRequest.getUpdates()) {
      String collection = getCollection(entry.getValue());
      if (collectionMetacardMap.containsKey(collection)) {
        List<Entry<Serializable, Metacard>> collectionEntries =
            collectionMetacardMap.get(collection);
        collectionEntries.add(entry);
      } else {
        List<Entry<Serializable, Metacard>> collectionEntries = new ArrayList<>();
        collectionEntries.add(entry);
        collectionMetacardMap.put(collection, collectionEntries);
      }
    }
    for (Map.Entry<String, List<Entry<Serializable, Metacard>>> entry :
        collectionMetacardMap.entrySet()) {
      UpdateRequest request =
          new UpdateRequestImpl(
              entry.getValue(),
              updateRequest.getAttributeName(),
              updateRequest.getProperties(),
              updateRequest.getStoreIds());
      updateRequests.put(entry.getKey(), request);
    }

    return updateRequests;
  }

  private void ensureAliasExists(String alias, String collectionPrefix) {
    for (IndexCollectionProvider provider : indexCollectionProviders) {
      clientFactory.addCollectionToAlias(alias, provider.getCollection(null), collectionPrefix);
    }
  }

  private void ensureDefaultCollectionExists() {
    for (IndexCollectionProvider provider : indexCollectionProviders) {
      String collection = provider.getCollection(null);
      createCollectionIfRequired(collection, provider);
    }
  }

  private void initializeKnownProviders() {
    catalogProviders.computeIfAbsent(QUERY_ALIAS, this::newProvider);
    List<String> collections = clientFactory.getCollectionsForAlias(QUERY_ALIAS);
    for (String collection : collections) {
      catalogProviders.computeIfAbsent(collection, this::newProvider);
    }
  }

  private List<BaseSolrCatalogProvider> getAliasProviderNonAlias() {
    List<BaseSolrCatalogProvider> providers = new ArrayList<>(catalogProviders.size());
    for (Map.Entry<String, BaseSolrCatalogProvider> entry : catalogProviders.entrySet()) {
      if (!entry.getKey().equals(QUERY_ALIAS)) {
        providers.add(entry.getValue());
      }
    }

    return providers;
  }

  private void initThreads() {
    if (queryExecutor != null) {
      destroy();
    }
    int numThreads =
        NumberUtils.toInt(
            AccessController.doPrivileged(
                (PrivilegedAction<String>)
                    () -> System.getProperty("org.codice.ddf.system.threadPoolSize")),
            128);
    queryExecutor =
        new ThreadPoolExecutor(
            numThreads,
            numThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<>(MAX_Q_SIZE),
            StandardThreadFactoryBuilder.newThreadFactory(QUERY_POOL_NAME),
            new ThreadPoolExecutor.CallerRunsPolicy());

    queryExecutor.prestartAllCoreThreads();
  }

  private void destroy() {
    queryExecutor.shutdown();

    try {
      boolean shutdown = queryExecutor.awaitTermination(30L, TimeUnit.SECONDS);
      if (!shutdown) {
        queryExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(QUERY_POOL_NAME + " graceful shutdown interrupted.", e);
    }

    queryExecutor = null;
  }
}
