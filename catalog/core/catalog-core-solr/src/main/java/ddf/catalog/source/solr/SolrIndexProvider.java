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
package ddf.catalog.source.solr;

import ddf.catalog.data.ContentType;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.DeleteResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IndexProvider;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.SourceMonitor;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.util.impl.DescribableImpl;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.codice.solr.factory.SolrClientFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class SolrIndexProvider extends DescribableImpl implements IndexProvider {

  protected static final String SOLR_CATALOG_CORE_NAME = "catalog";

  private Map<String, BaseSolrCatalogProvider> catalogProviders = new HashMap<>();

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
    Validate.notNull(clientFactory, "SolrClient cannot be null.");
    Validate.notNull(adapter, "FilterAdapter cannot be null");
    Validate.notNull(solrFilterDelegateFactory, "SolrFilterDelegateFactory cannot be null");
    Validate.notNull(resolver, "DynamicSchemaResolver cannot be null");

    /** Create storage collection to provider map */
    catalogProviders.put(
        SOLR_CATALOG_CORE_NAME,
        new BaseSolrCatalogProvider(
            clientFactory.newClient(SOLR_CATALOG_CORE_NAME),
            adapter,
            solrFilterDelegateFactory,
            resolver));
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

  @Override
  public CreateResponse create(CreateRequest createRequest) throws IngestException {
    return catalogProviders.get(SOLR_CATALOG_CORE_NAME).create(createRequest);
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    return null;
  }

  @Override
  public DeleteResponse delete(DeleteRequest deleteRequest) throws IngestException {
    return catalogProviders.get(SOLR_CATALOG_CORE_NAME).delete(deleteRequest);
  }

  @Override
  public SourceResponse query(QueryRequest request) throws UnsupportedQueryException {
    return catalogProviders.get(SOLR_CATALOG_CORE_NAME).query(request);
  }

  @Override
  public boolean isAvailable() {
    try {
      return catalogProviders
          .get(SOLR_CATALOG_CORE_NAME)
          .getSolrClient()
          .isAvailable(30L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      return false;
    }
  }

  public void shutdown() {
    this.catalogProviders.forEach((k, p) -> p.shutdown());
  }

  @Override
  public boolean isAvailable(SourceMonitor callback) {
    return catalogProviders.get(SOLR_CATALOG_CORE_NAME).isAvailable(callback);
  }

  @Override
  public Set<ContentType> getContentTypes() {
    return catalogProviders.get(SOLR_CATALOG_CORE_NAME).getContentTypes();
  }

  @Override
  public void maskId(String id) {
    catalogProviders.forEach((k, p) -> p.maskId(id));
  }
}
