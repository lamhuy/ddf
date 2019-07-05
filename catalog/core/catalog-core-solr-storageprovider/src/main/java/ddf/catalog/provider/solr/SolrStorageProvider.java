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

import ddf.catalog.data.ContentType;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.filter.delegate.TagsFilterDelegate;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.DeleteResponse;
import ddf.catalog.operation.IndexDeleteResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.Request;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateResponseImpl;
import ddf.catalog.operation.impl.DeleteResponseImpl;
import ddf.catalog.operation.impl.UpdateResponseImpl;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.SourceMonitor;
import ddf.catalog.source.StorageProvider;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import ddf.catalog.util.impl.DescribableImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.codice.solr.factory.SolrClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class SolrStorageProvider extends DescribableImpl implements StorageProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrStorageProvider.class);

  private List<String> parameters;

  private Map<String, String> tagToCore = new HashMap<>();

  protected static final String DEFAULT_SOLR_CATALOG_CORE = "catalog";

  private Map<String, BaseSolrCatalogProvider> catalogProviders = new HashMap<>();

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
  public SolrStorageProvider(
      SolrClientFactory clientFactory,
      FilterAdapter adapter,
      SolrFilterDelegateFactory solrFilterDelegateFactory,
      DynamicSchemaResolver resolver) {
    Validate.notNull(clientFactory, "SolrClient cannot be null.");
    Validate.notNull(adapter, "FilterAdapter cannot be null");
    Validate.notNull(solrFilterDelegateFactory, "SolrFilterDelegateFactory cannot be null");
    Validate.notNull(resolver, "DynamicSchemaResolver cannot be null");

    this.clientFactory = clientFactory;
    this.filterAdapter = adapter;
    this.solrFilterDelegateFactory = solrFilterDelegateFactory;
    this.resolver = resolver;

    /** Create storage collection to provider map */
    catalogProviders.put(DEFAULT_SOLR_CATALOG_CORE, newProvider(DEFAULT_SOLR_CATALOG_CORE));
  }

  /**
   * Convenience constructor that creates a new ddf.catalog.source.solr.DynamicSchemaResolver
   *
   * @param clientFactory Solr client factory
   * @param adapter injected implementation of FilterAdapter
   */
  public SolrStorageProvider(
      SolrClientFactory clientFactory,
      FilterAdapter adapter,
      SolrFilterDelegateFactory solrFilterDelegateFactory) {
    this(clientFactory, adapter, solrFilterDelegateFactory, new DynamicSchemaResolver());
  }

  private BaseSolrCatalogProvider newProvider(String core) {
    return new BaseSolrCatalogProvider(
        clientFactory.getClient(core), filterAdapter, solrFilterDelegateFactory, resolver);
  }

  @Override
  public CreateResponse create(CreateRequest createRequest) throws IngestException {
    CatalogProvider cat = getCatalogProvider(createRequest);
    if (cat == null) {
      LOGGER.warn("Create request not executed");
      return new CreateResponseImpl(createRequest, null, new ArrayList<>());
    } else {
      return cat.create(createRequest);
    }
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    CatalogProvider cat = getCatalogProvider(updateRequest);
    if (cat == null) {
      LOGGER.warn("Update request not executed");
      return new UpdateResponseImpl(updateRequest, null, new ArrayList<>());
    } else {
      return cat.update(updateRequest);
    }
  }

  @Override
  public DeleteResponse delete(IndexDeleteResponse deleteRequest) throws IngestException {
    DeleteResponse response =
        new DeleteResponseImpl((DeleteRequest) deleteRequest.getRequest(), null, new ArrayList<>());
    // deleted indexes could spread across multiple storage collection
    for (Set<String> tags : deleteRequest.getTags()) {
      Optional<String> core = getCore(tags);
      if (core.isPresent()) {
        BaseSolrCatalogProvider cat = catalogProviders.get(core.get());
        if (cat == null) {
          LOGGER.warn(
              "Unable to find catalog provider for core: {}. Delete query not executed",
              core.get());
        } else {
          response.mergeResponse(cat.deleteByIds(deleteRequest.getIds(tags)));
        }
      } else {
        LOGGER.warn("Unable to find core mapping for these tags {}", tags.toString());
        return response;
      }
    }
    return response;
  }

  @Override
  public SourceResponse query(QueryRequest queryRequest) throws UnsupportedQueryException {
    CatalogProvider cat = getCatalogProvider(queryRequest);
    if (cat == null) {
      LOGGER.warn("Update request not executed");
      return null;
    } else {
      return cat.query(queryRequest);
    }
  }

  /**
   * for a solr storage provider quering by IDs might not be optimal. hence handling as a normal
   * query . alias is used to query against all collection....*
   */
  public SourceResponse queryByIds(QueryRequest queryRequest, List<String> ids)
      throws UnsupportedQueryException {
    return this.query(queryRequest);
  }

  public void shutdown() {
    LOGGER.debug("Closing down Solr client.");
    this.catalogProviders.forEach((k, p) -> p.shutdown());
  }

  @Override
  public boolean isAvailable() {
    return catalogProviders
        .values()
        .stream()
        .map(
            c -> {
              try {
                return c.isAvailable(30L, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                return false;
              }
            })
        .reduce(true, (a, b) -> a && b);
  }

  @Override
  public boolean isAvailable(SourceMonitor callback) {
    return catalogProviders
        .values()
        .stream()
        .map(c -> c.isAvailable(callback))
        .reduce(true, (a, b) -> a && b);
  }

  @Override
  public Set<ContentType> getContentTypes() {
    return catalogProviders
        .values()
        .stream()
        .map(c -> c.getContentTypes())
        .reduce(
            new HashSet<>(),
            (a, b) -> {
              a.addAll(b);
              return a;
            });
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
    for (String parameter : parameters) {
      String[] tagCorePair = parameter.split("=");
      String core = tagCorePair[1];
      tagToCore.put(tagCorePair[0], core);
      catalogProviders.computeIfAbsent(core, this::newProvider);
    }
  }

  /** assuming all metacards are of the same type /tag for each request * */
  private CatalogProvider getCatalogProvider(Request request) {
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
      try {
        for (String tag : tagToCore.keySet()) {
          if (filterAdapter.adapt(
              ((QueryRequest) request).getQuery(), new TagsFilterDelegate(tag))) {
            tags.add(tag);
            break;
          }
        }
      } catch (UnsupportedQueryException e) {
        LOGGER.warn("Unable to find tab for the query request", DEFAULT_SOLR_CATALOG_CORE);
      }
    }

    Optional<String> core = getCore(tags);
    if (core.isPresent()) {
      return catalogProviders.get(core.get());
    } else {
      LOGGER.warn("Unable to find core for the request");
      return null;
    }
  }

  /**
   * From a list of tags extracted from a metacard, return the associate solr core Assuming
   *
   * @param tags all tags from a metacard
   * @return the first associate solr core for those given tags
   */
  private Optional<String> getCore(Set<String> tags) {
    return tags.stream().map(t -> tagToCore.get(t)).findFirst();
    // .orElseGet(() -> { LOGGER.warn("Unable to find core for the given tags, using default core
    // {}", DEFAULT_SOLR_CATALOG_CORE); return DEFAULT_SOLR_CATALOG_CORE;});
  }
}
