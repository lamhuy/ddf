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

import ddf.catalog.data.BinaryContent;
import ddf.catalog.data.ContentType;
import ddf.catalog.data.Metacard;
import ddf.catalog.data.Result;
import ddf.catalog.data.impl.AttributeImpl;
import ddf.catalog.data.impl.ResultImpl;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.DeleteResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.Update;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateResponseImpl;
import ddf.catalog.operation.impl.SourceResponseImpl;
import ddf.catalog.operation.impl.UpdateImpl;
import ddf.catalog.operation.impl.UpdateResponseImpl;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.SourceMonitor;
import ddf.catalog.source.StorageProvider;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import ddf.catalog.source.solr.SolrMetacardClient;
import ddf.catalog.transform.CatalogTransformerException;
import ddf.catalog.transform.InputTransformer;
import ddf.catalog.transform.MetacardTransformer;
import ddf.catalog.util.impl.DescribableImpl;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.codice.solr.factory.SolrClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class SolrStorageProvider extends DescribableImpl implements StorageProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrStorageProvider.class);

  protected static final String DEFAULT_SOLR_CATALOG_STORE = "catalog_store";

  protected static final String ID_FIELD = "id_txt";

  protected static final String DATA_FIELD = "metacard-data_txt";

  protected BaseSolrCatalogProvider provider;

  protected SolrClientFactory clientFactory;

  protected InputTransformer metacardDecodeTransformer;

  protected MetacardTransformer metacardEncodeTransformer;

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
      DynamicSchemaResolver resolver,
      InputTransformer metacardDecodeTransformer,
      MetacardTransformer metacardEncodeTransformer) {
    Validate.notNull(clientFactory, "SolrClientFactory cannot be null.");
    Validate.notNull(adapter, "FilterAdapter cannot be null");
    Validate.notNull(solrFilterDelegateFactory, "SolrFilterDelegateFactory cannot be null");
    Validate.notNull(resolver, "DynamicSchemaResolver cannot be null");
    Validate.notNull(metacardDecodeTransformer, "MetacardDecodeTransformer cannot be null.");
    Validate.notNull(metacardEncodeTransformer, "MetacardEncodeTransformer cannot be null.");

    this.clientFactory = clientFactory;
    this.metacardDecodeTransformer = metacardDecodeTransformer;
    this.metacardEncodeTransformer = metacardEncodeTransformer;

    /** Create storage collection to provider map */
    provider =
        new BaseSolrCatalogProvider(
            this.clientFactory.getClient(DEFAULT_SOLR_CATALOG_STORE),
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
  public SolrStorageProvider(
      SolrClientFactory clientFactory,
      FilterAdapter adapter,
      SolrFilterDelegateFactory solrFilterDelegateFactory,
      InputTransformer metacardDecodeTransformer,
      MetacardTransformer metacardEncodeTransformer) {
    this(
        clientFactory,
        adapter,
        solrFilterDelegateFactory,
        new DynamicSchemaResolver(),
        metacardDecodeTransformer,
        metacardEncodeTransformer);
  }

  @Override
  public CreateResponse create(CreateRequest createRequest) throws IngestException {
    List<Metacard> metacards = createRequest.getMetacards();
    putMetacards(metacards);
    return new CreateResponseImpl(createRequest, createRequest.getProperties(), metacards);
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    if (!StringUtils.equals(updateRequest.getAttributeName(), Metacard.ID)) {
      throw new IngestException("Unable to update by anything other than Metacard.ID");
    }
    List<Map.Entry<Serializable, Metacard>> requestedUpdates = updateRequest.getUpdates();

    try {
      Set<String> identifiers =
          requestedUpdates
              .stream()
              .map(Map.Entry::getKey)
              .map(Serializable::toString)
              .filter(Objects::nonNull)
              .collect(Collectors.toSet());
      Map<String, Metacard> oldMetacardMap =
          getMetacards(identifiers)
              .stream()
              .filter(Objects::nonNull)
              .collect(Collectors.toMap(Metacard::getId, Function.identity()));

      List<Metacard> updatedCards =
          requestedUpdates
              .stream()
              .map(Map.Entry::getValue)
              .peek(mc -> mc.setSourceId(getId()))
              .collect(Collectors.toList());
      putMetacards(updatedCards);
      List<Update> updates =
          updatedCards
              .stream()
              .map(mc -> new UpdateImpl(mc, oldMetacardMap.get(mc.getId())))
              .collect(Collectors.toList());
      return new UpdateResponseImpl(updateRequest, updateRequest.getProperties(), updates);
    } catch (UnsupportedQueryException e) {
      throw new IngestException(e);
    }
  }

  @Override
  public DeleteResponse delete(DeleteRequest deleteRequest) throws IngestException {
    String deleteAttribute = deleteRequest.getAttributeName();
    if (!StringUtils.equals(deleteAttribute, Metacard.ID)) {
      throw new IngestException(
          "Delete from SolrStorageProvider by anything other than Metacard.ID not supported");
    }

    // Once it's an ID delete, we can just delegate to existing provider as it can handle the
    // storage as well
    return provider.delete(deleteRequest);
  }

  @Override
  public SourceResponse query(QueryRequest queryRequest) throws UnsupportedQueryException {
    throw new UnsupportedQueryException("Generic queries to SolrStorageProvider not supported.");
  }

  /**
   * for a solr storage provider quering by IDs might not be optimal. hence handling as a normal
   * query
   */
  public SourceResponse queryByIds(
      QueryRequest queryRequest, Map<String, Serializable> properties, List<String> ids)
      throws UnsupportedQueryException {
    List<Metacard> metacards = getMetacards(new HashSet<>(ids));
    List<Result> results = metacards.stream().map(ResultImpl::new).collect(Collectors.toList());
    return new SourceResponseImpl(queryRequest, properties, results);
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

  private void putMetacards(List<Metacard> metacards) throws IngestException {
    SolrMetacardClient client = provider.getSolrMetacardClient();

    for (Metacard metacard : metacards) {
      boolean isSourceIdSet =
          (metacard.getSourceId() != null && !"".equals(metacard.getSourceId()));

      if (StringUtils.isBlank(metacard.getId())) {
        if (isSourceIdSet) {
          throw new IngestException("Metacard from a separate distribution must have ID");
        }
        metacard.setAttribute(new AttributeImpl(Metacard.ID, provider.generatePrimaryKey()));
      }

      if (!isSourceIdSet) {
        metacard.setSourceId(getId());
      }

      try {
        SolrInputDocument solrDoc = getSolrDoc(metacard);
        if (solrDoc != null) {
          client.commit(Collections.singletonList(solrDoc), true, client.isNrtType(metacard));
        } else {
          LOGGER.debug("Could not get Solr Doc for metacard: {}", metacard);
        }
      } catch (CatalogTransformerException | IOException | SolrServerException e) {
        throw new IngestException(e);
      }
    }
  }

  private SolrInputDocument getSolrDoc(Metacard metacard) throws CatalogTransformerException {
    SolrInputDocument solrInputDocument = new SolrInputDocument();
    solrInputDocument.addField(ID_FIELD, metacard.getId());
    String encodedMetacard = encodeMetacard(metacard);
    if (encodedMetacard == null) {
      LOGGER.debug("Unable to encode metacard: {}, returning null SolrDoc", metacard);
      return null;
    }
    solrInputDocument.addField(DATA_FIELD, encodedMetacard);
    return solrInputDocument;
  }

  protected String encodeMetacard(Metacard metacard) throws CatalogTransformerException {
    BinaryContent binaryContent = metacardEncodeTransformer.transform(metacard, null);
    String encodedMetacard = null;
    if (binaryContent.getMimeType().getPrimaryType().equals("text")) {
      try {
        encodedMetacard = new String(binaryContent.getByteArray());
      } catch (IOException e) {
        LOGGER.debug("Unable to get metacard data", e);
      }
    }
    return encodedMetacard;
  }

  private Metacard getMetacard(SolrDocument solrDoc) {
    Object idField = solrDoc.getFirstValue(ID_FIELD);
    if (idField != null) {
      String id = idField.toString();

      Object dataField = solrDoc.getFirstValue(DATA_FIELD);
      if (dataField != null) {
        String data = dataField.toString();
        try {
          return metacardDecodeTransformer.transform(new ByteArrayInputStream(data.getBytes()), id);
        } catch (IOException | CatalogTransformerException e) {
          LOGGER.debug("Unable to transform {} to metacard", data, e);
        }
      }
    }
    return null;
  }

  private List<Metacard> getMetacards(Set<String> ids) throws UnsupportedQueryException {
    SolrMetacardClient client = provider.getSolrMetacardClient();
    List<SolrDocument> solrDocs = client.getSolrDocs(ids);
    return solrDocs
        .stream()
        .map(this::getMetacard)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
