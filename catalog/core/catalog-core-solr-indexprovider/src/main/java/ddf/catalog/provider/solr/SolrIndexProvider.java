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
import ddf.catalog.operation.IndexDeleteResponse;
import ddf.catalog.operation.IndexQueryResponse;
import ddf.catalog.operation.QueryRequest;
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
import org.apache.commons.lang.Validate;
import org.codice.solr.factory.SolrClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class SolrIndexProvider extends DescribableImpl implements IndexProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexProvider.class);

  protected static final String DEFAULT_SOLR_INDEX_CORE = "catalog";

  private final BaseSolrCatalogProvider catalogProviders;

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
    catalogProviders =
        new BaseSolrCatalogProvider(
            clientFactory.getClient(DEFAULT_SOLR_INDEX_CORE),
            adapter,
            solrFilterDelegateFactory,
            resolver);
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
    return catalogProviders.create(createRequest);
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    return catalogProviders.update(updateRequest);
  }

  @Override
  public IndexDeleteResponse delete(DeleteRequest deleteRequest) throws IngestException {
    return catalogProviders.deleteIndex(deleteRequest);
  }

  @Override
  public IndexQueryResponse query(QueryRequest request) throws UnsupportedQueryException {
    return catalogProviders.queryIndex(request);
  }

  public void shutdown() {
    LOGGER.debug("Closing down Solr client.");
    this.catalogProviders.shutdown();
  }

  @Override
  public void maskId(String id) {
    catalogProviders.maskId(id);
  }
}
