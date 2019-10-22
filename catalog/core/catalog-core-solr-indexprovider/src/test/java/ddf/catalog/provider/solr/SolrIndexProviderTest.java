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

import static ddf.catalog.Constants.COLLECTION_HINT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.impl.AttributeImpl;
import ddf.catalog.data.types.Core;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.filter.FilterBuilder;
import ddf.catalog.filter.proxy.builder.GeotoolsFilterBuilder;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.IndexQueryResponse;
import ddf.catalog.operation.IndexQueryResult;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.CreateResponseImpl;
import ddf.catalog.operation.impl.DeleteRequestImpl;
import ddf.catalog.operation.impl.DeleteResponseImpl;
import ddf.catalog.operation.impl.IndexQueryResponseImpl;
import ddf.catalog.operation.impl.IndexQueryResultImpl;
import ddf.catalog.operation.impl.QueryImpl;
import ddf.catalog.operation.impl.QueryRequestImpl;
import ddf.catalog.operation.impl.UpdateRequestImpl;
import ddf.catalog.operation.impl.UpdateResponseImpl;
import ddf.catalog.source.IndexProvider;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.ConfigurationStore;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import ddf.catalog.source.solr.api.IndexCollectionProvider;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.codice.solr.client.solrj.SolrClient;
import org.codice.solr.factory.SolrClientFactory;
import org.junit.Before;
import org.junit.Test;
import org.opengis.filter.Filter;

public class SolrIndexProviderTest {

  private FilterBuilder filterBuilder;

  private BaseSolrCatalogProvider catalogProvider;

  private SolrIndexProvider indexProvider;

  private FilterAdapter filterAdapter;

  private SolrClient solrClientMock;

  private SolrClientFactory cloudClientMock;

  public SolrIndexProviderTest() {}

  @Before
  public void setUp() throws Exception {
    cloudClientMock = mock(SolrClientFactory.class);
    solrClientMock = mock(SolrClient.class);
    filterAdapter = mock(FilterAdapter.class);
    filterBuilder = new GeotoolsFilterBuilder();

    when(cloudClientMock.isAvailable()).thenReturn(true);
    when(cloudClientMock.isSolrCloud()).thenReturn(true);
    when(cloudClientMock.newClient(any())).thenReturn(solrClientMock);
    IndexCollectionProvider collectionProvider = mock(IndexCollectionProvider.class);

    indexProvider =
        new SolrIndexProvider(
            cloudClientMock,
            filterAdapter,
            mock(SolrFilterDelegateFactory.class),
            mock(DynamicSchemaResolver.class),
            Collections.singletonList(collectionProvider),
            Collections.emptyList());
    indexProvider.setCollectionThreadWorkaround(false);
    indexProvider.setGetHandlerWorkaround(false);
    indexProvider.setCollectionAlias("catalog");

    catalogProvider = mock(BaseSolrCatalogProvider.class);
  }

  @Test
  public void testCreate() throws Exception {
    indexProvider.catalogProviders.put("catalog_index", catalogProvider);
    CreateResponse response = createRecord(indexProvider);
    assertThat(response.getCreatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testQuery() throws Exception {
    indexProvider.catalogProviders.put("catalog", catalogProvider);

    Filter filter = filterBuilder.attribute("anyText").is().like().text("*");
    List<Metacard> records = getTestRecords();

    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));
    when(catalogProvider.queryIndex(request))
        .thenReturn(
            new IndexQueryResponseImpl(
                request,
                records.stream().map(m -> getScoredResult(m.getId())).collect(Collectors.toList()),
                Long.valueOf(records.size())));

    IndexQueryResponse response = indexProvider.query(request);
    assertThat(response.getHits(), equalTo(2L));
  }

  @Test
  public void testRealTimeQuery() throws Exception {
    indexProvider.catalogProviders.put("catalog", catalogProvider);
    when(filterAdapter.adapt(any(), any())).thenReturn(true);

    Filter filter = filterBuilder.attribute(Metacard.ID).is().like().text("1234");
    List<Metacard> records = getTestRecords();

    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));
    when(catalogProvider.queryIndexCache(any()))
        .thenReturn(
            new IndexQueryResponseImpl(
                request,
                records.stream().map(m -> getScoredResult(m.getId())).collect(Collectors.toList()),
                Long.valueOf(records.size())));

    IndexQueryResponse response = indexProvider.query(request);
    assertThat(response.getHits(), equalTo(2L));
  }

  @Test
  public void testUpdate() throws Exception {
    indexProvider.catalogProviders.put("catalog", catalogProvider);
    indexProvider.catalogProviders.put("catalog_index", catalogProvider);

    List<Metacard> records = getTestRecords();

    UpdateRequest request =
        new UpdateRequestImpl(
            records.stream().map(Metacard::getId).toArray(String[]::new), records);

    when(catalogProvider.update(any()))
        .thenReturn(new UpdateResponseImpl(request, request.getProperties(), records, records));
    UpdateResponse response = indexProvider.update(request);
    assertThat(response.getUpdatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testDelete() throws Exception {
    indexProvider.catalogProviders.put("catalog", catalogProvider);
    List<Metacard> records = getTestRecords();
    DeleteRequest deleteRequest =
        new DeleteRequestImpl(records.stream().map(Metacard::getId).toArray(String[]::new));

    when(catalogProvider.delete(any()))
        .thenReturn(new DeleteResponseImpl(deleteRequest, Collections.emptyMap(), records));

    indexProvider.delete(deleteRequest);
  }

  @Test
  public void testIsAvailableFalse() {
    when(cloudClientMock.isAvailable()).thenReturn(false);
    boolean avail = indexProvider.isAvailable();
    assertThat(avail, is(false));
  }

  @Test
  public void testIsAvailableTrue() {
    boolean avail = indexProvider.isAvailable();
    assertThat(avail, is(true));
  }

  @Test
  public void testForceAutoCommit() {
    indexProvider.setForceAutoCommit(true);
    assertThat(ConfigurationStore.getInstance().isForceAutoCommit(), is(true));
  }

  @Test
  public void testNonCloudCreate() throws Exception {
    indexProvider.catalogProviders.put("catalog_index", catalogProvider);
    when(cloudClientMock.isSolrCloud()).thenReturn(false);
    testCreate();
  }

  @Test
  public void testNonCloudUpdate() throws Exception {
    indexProvider.catalogProviders.put("catalog_index", catalogProvider);
    when(cloudClientMock.isSolrCloud()).thenReturn(false);
    testUpdate();
  }

  @Test
  public void testHintedCollectionQuery() throws Exception {
    BaseSolrCatalogProvider provider2 = mock(BaseSolrCatalogProvider.class);

    indexProvider.catalogProviders.put("catalog", catalogProvider);
    indexProvider.catalogProviders.put("mycollection", provider2);

    Filter filter = filterBuilder.attribute("anyText").is().like().text("*");

    Map<String, Serializable> properties = new HashMap<>();
    properties.put(COLLECTION_HINT, "mycollection");
    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter), properties);

    indexProvider.query(request);
    verify(catalogProvider, times(0)).queryIndex(any());
    verify(provider2, times(1)).queryIndex(any());
  }

  @Test
  public void testHintedCollectionIndexQuery() throws Exception {
    BaseSolrCatalogProvider provider2 = mock(BaseSolrCatalogProvider.class);

    indexProvider.catalogProviders.put("catalog", catalogProvider);
    indexProvider.catalogProviders.put("mycollection", provider2);

    Filter filter = filterBuilder.attribute("anyText").is().like().text("*");
    Map<String, Serializable> properties = new HashMap<>();
    properties.put(COLLECTION_HINT, "mycollection");
    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter), properties);

    indexProvider.query(request);
    verify(catalogProvider, times(0)).queryIndex(any());
    verify(provider2, times(1)).queryIndex(any());
  }

  @Test
  public void testRefresh() {
    final String alias = "mycatalog";
    Map<String, Object> props = new HashMap<>();
    props.put(SolrIndexProvider.COLLECTION_THREAD_WORKAROUND_PROP, Boolean.TRUE);
    props.put(SolrIndexProvider.GET_HANDLER_WORKAROUND_PROP, Boolean.TRUE);
    props.put(SolrIndexProvider.COLLECTION_ALIAS_PROP, alias);
    indexProvider.refresh(props);

    assertThat(indexProvider.isGetHandlerWorkaround(), is(true));
    assertThat(indexProvider.isCollectionThreadWorkaround(), is(true));
    assertThat(indexProvider.getCollectionAlias(), is(alias));
  }

  private CreateResponse createRecord(IndexProvider provider) throws Exception {
    List<Metacard> list =
        Arrays.asList(
            MockMetacard.createMetacard(Library.getFlagstaffRecord()),
            MockMetacard.createMetacard(Library.getTampaRecord()));

    CreateRequest request = new CreateRequestImpl(list);
    when(catalogProvider.create(any()))
        .thenReturn(new CreateResponseImpl(request, request.getProperties(), list));

    return provider.create(request);
  }

  private List<Metacard> getTestRecords() {
    Metacard metacard1 = MockMetacard.createMetacard(Library.getFlagstaffRecord());
    metacard1.setAttribute(new AttributeImpl(Core.ID, "1"));
    Metacard metacard2 = MockMetacard.createMetacard(Library.getTampaRecord());
    metacard2.setAttribute(new AttributeImpl(Core.ID, "2"));
    return Arrays.asList(metacard1, metacard2);
  }

  private IndexQueryResult getScoredResult(String id) {
    return new IndexQueryResultImpl(id, 1.0);
  }

  @Test
  public void testShutdown() {
    indexProvider.shutdown();
  }

  @Test
  public void testMaskId() {
    indexProvider.maskId("id");
  }
}
