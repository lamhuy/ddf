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
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.DeleteResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.SourceMonitor;
import ddf.catalog.source.StorageProvider;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import ddf.catalog.util.impl.DescribableImpl;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.Validate;
import org.codice.solr.factory.SolrClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class SolrStorageProvider extends DescribableImpl implements StorageProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrStorageProvider.class);

  protected static final String DEFAULT_SOLR_CATALOG_CORE = "catalog";

  BaseSolrCatalogProvider provider;

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

    /** Create storage collection to provider map */
    provider =
        new BaseSolrCatalogProvider(
            clientFactory.getClient(DEFAULT_SOLR_CATALOG_CORE),
            adapter,
            solrFilterDelegateFactory,
            resolver);
  }

  @Override
  public CreateResponse create(CreateRequest createRequest) throws IngestException {
    return provider.create(createRequest);
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    return provider.update(updateRequest);
  }

  @Override
  public DeleteResponse delete(DeleteRequest deleteRequest) throws IngestException {
    return provider.delete(deleteRequest);
  }

  @Override
  public SourceResponse query(QueryRequest queryRequest) throws UnsupportedQueryException {
    return provider.query(queryRequest);
  }

  /**
   * for a solr storage provider quering by IDs might not be optimal. hence handling as a normal
   * query
   */
  public SourceResponse queryByIds(QueryRequest queryRequest, List<String> ids)
      throws UnsupportedQueryException {
    return this.query(queryRequest);
  }

  public void shutdown() {
    LOGGER.debug("Closing down Solr client.");
    provider.shutdown();
  }

  @Override
  public boolean isAvailable() {
    return provider.isAvailable();
  }

  @Override
  public boolean isAvailable(SourceMonitor callback) {
    return provider.isAvailable(callback);
  }

  @Override
  public Set<ContentType> getContentTypes() {
    return provider.getContentTypes();
  }

  @Override
  public void maskId(String id) {
    provider.maskId(id);
  }
}
