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
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.DeleteResponse;
import ddf.catalog.operation.IndexQueryResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.QueryResponseImpl;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IndexProvider;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.SourceMonitor;
import ddf.catalog.source.StorageProvider;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.util.impl.MaskableImpl;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common base class for all Catalog providers. This class utilize two providers: one for handling
 * index, one for handling the storage of data
 */
public abstract class AbstractCatalogProvider extends MaskableImpl implements CatalogProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCatalogProvider.class);

  private static final String DESCRIBABLE_PROPERTIES_FILE = "/describable.properties";

  private static final Properties DESCRIBABLE_PROPERTIES = new Properties();

  static {
    try (InputStream inputStream =
        AbstractCatalogProvider.class.getResourceAsStream(DESCRIBABLE_PROPERTIES_FILE)) {
      DESCRIBABLE_PROPERTIES.load(inputStream);
    } catch (IOException e) {
      LOGGER.info("Did not load properties properly.", e);
    }
  }

  private IndexProvider indexProvider;
  private StorageProvider storageProvider;

  /** Constructor. */
  public AbstractCatalogProvider(IndexProvider indexProvider, StorageProvider storageProvider) {

    this.indexProvider = indexProvider;
    this.storageProvider = storageProvider;
    indexProvider.maskId(getId());
    storageProvider.maskId(getId());
  }

  @Override
  public void maskId(String id) {
    super.maskId(id);
    indexProvider.maskId(id);
    storageProvider.maskId(id);
  }

  /**
   * Used to signal to the Solr client to commit on every transaction. Updates the underlying {@link
   * ConfigurationStore} so that the property is propagated throughout the Solr Catalog Provider
   * code.
   *
   * @param forceAutoCommit {@code true} to force auto-commits
   */
  public void setForceAutoCommit(boolean forceAutoCommit) {
    ConfigurationStore.getInstance().setForceAutoCommit(forceAutoCommit);
  }

  /**
   * Disables text path indexing for every subsequent update or insert.
   *
   * @param disableTextPath {@code true} to turn off text path indexing
   */
  public void setDisableTextPath(boolean disableTextPath) {
    ConfigurationStore.getInstance().setDisableTextPath(disableTextPath);
  }

  @Override
  public Set<ContentType> getContentTypes() {
    return storageProvider.getContentTypes();
  }

  @Override
  public boolean isAvailable() {
    return storageProvider.isAvailable();
  }

  @Override
  public boolean isAvailable(SourceMonitor callback) {
    return storageProvider.isAvailable(callback);
  }

  @Override
  public SourceResponse query(QueryRequest queryRequest) throws UnsupportedQueryException {
    long startTime = System.currentTimeMillis();
    IndexQueryResponse indexQueryResponse = indexProvider.query(queryRequest);
    long indexElapsedTime = System.currentTimeMillis() - startTime;
    SourceResponse queryResponse = null;
    if (indexQueryResponse != null) {
      if (CollectionUtils.isNotEmpty(indexQueryResponse.getIds())) {
        queryResponse =
            storageProvider.queryByIds(
                queryRequest, indexQueryResponse.getProperties(), indexQueryResponse.getIds());
      } else {
        queryResponse =
            new QueryResponseImpl(
                queryRequest, Collections.emptyList(), true, 0, indexQueryResponse.getProperties());
      }
    }
    long totalElapsedTime = System.currentTimeMillis() - startTime;
    LOGGER.trace(
        "Query Index elapsed time {} and query storage elapsed time {}",
        indexElapsedTime,
        totalElapsedTime - indexElapsedTime);
    return queryResponse;
  }

  @Override
  public String getDescription() {
    return DESCRIBABLE_PROPERTIES.getProperty("description", "");
  }

  @Override
  public String getOrganization() {
    return DESCRIBABLE_PROPERTIES.getProperty("organization", "");
  }

  @Override
  public String getTitle() {
    return DESCRIBABLE_PROPERTIES.getProperty("name", "");
  }

  @Override
  public String getVersion() {
    return DESCRIBABLE_PROPERTIES.getProperty("version", "");
  }

  @Override
  public CreateResponse create(CreateRequest createRequest) throws IngestException {
    long startTime = System.currentTimeMillis();
    CreateResponse createResponse = storageProvider.create(createRequest);
    long indexElapsedTime = System.currentTimeMillis() - startTime;
    // create index only for those inserted metacard
    if (createResponse != null && !createResponse.getCreatedMetacards().isEmpty()) {
      CreateRequest indexRequest = new CreateRequestImpl(createResponse.getCreatedMetacards());
      indexProvider.create(indexRequest);
    }
    long totalElapsedTime = System.currentTimeMillis() - startTime;
    LOGGER.trace(
        "Create Index elapsed time {} and create storage elapsed time {}",
        indexElapsedTime,
        totalElapsedTime - indexElapsedTime);
    return createResponse;
  }

  @Override
  public DeleteResponse delete(DeleteRequest deleteRequest) throws IngestException {
    indexProvider.delete(deleteRequest);
    DeleteResponse deleteResponse = storageProvider.delete(deleteRequest);
    return deleteResponse;
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    storageProvider.update(updateRequest);
    return indexProvider.update(updateRequest);
  }

  public void setStorageProvider(StorageProvider storageProvider) {
    LOGGER.warn("Setting storage provider: {}", storageProvider.getClass().getName());
    storageProvider.maskId(getId());
    this.storageProvider = storageProvider;
  }

  public void setIndexProvider(IndexProvider indexProvider) {
    LOGGER.warn("Setting index provider: {}", indexProvider.getClass().getName());
    indexProvider.maskId(getId());
    this.indexProvider = indexProvider;
  }

  /** Shuts down the connection to Solr and releases resources. */
  public void shutdown() {
    indexProvider.shutdown();
    storageProvider.shutdown();
  }
}
