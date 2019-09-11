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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ddf.catalog.data.Metacard;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.filter.FilterBuilder;
import ddf.catalog.filter.proxy.builder.GeotoolsFilterBuilder;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.IndexQueryResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.CreateResponseImpl;
import ddf.catalog.operation.impl.DeleteRequestImpl;
import ddf.catalog.operation.impl.IndexQueryResponseImpl;
import ddf.catalog.operation.impl.QueryImpl;
import ddf.catalog.operation.impl.QueryRequestImpl;
import ddf.catalog.operation.impl.UpdateRequestImpl;
import ddf.catalog.operation.impl.UpdateResponseImpl;
import ddf.catalog.provider.solr.defaultindexcollectionprovider.DefaultSolrIndexCollectionProvider;
import ddf.catalog.source.IndexProvider;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.codice.solr.client.solrj.SolrClient;
import org.codice.solr.factory.SolrClientFactory;
import org.junit.Before;
import org.junit.Test;
import org.opengis.filter.Filter;

public class SolrIndexProviderTest {

  private FilterBuilder filterBuilder = new GeotoolsFilterBuilder();
  private BaseSolrCatalogProvider catalogProvider;
  private SolrIndexProvider indexProvider;

  public SolrIndexProviderTest() {}

  @Before
  public void setup() {
    SolrClient solrClientMock = mock(SolrClient.class);
    SolrClientFactory cloudClientMock = mock(SolrClientFactory.class);
    when(cloudClientMock.isSolrCloud()).thenReturn(true);
    when(cloudClientMock.newClient(any())).thenReturn(solrClientMock);
    indexProvider =
        new SolrIndexProvider(
            cloudClientMock,
            mock(FilterAdapter.class),
            mock(SolrFilterDelegateFactory.class),
            mock(DynamicSchemaResolver.class),
            Collections.singletonList(new DefaultSolrIndexCollectionProvider()),
            Collections.emptyList());

    catalogProvider = mock(BaseSolrCatalogProvider.class);
  }

  @Test
  public void testCreate() throws Exception {
    CreateResponse response = createRecord(indexProvider);
    assertThat(response.getCreatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testQuery() throws Exception {
    CreateResponse records = createRecord(indexProvider);
    Filter filter = filterBuilder.attribute("anyText").is().like().text("*");
    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));
    when(catalogProvider.queryIndex(request))
        .thenReturn(
            new IndexQueryResponseImpl(
                request,
                records
                    .getCreatedMetacards()
                    .stream()
                    .map(Metacard::getId)
                    .collect(Collectors.toList()),
                2L));

    IndexQueryResponse response = indexProvider.query(request);
    assertThat(response.getHits(), equalTo(2L));
  }

  @Test
  public void testUpdate() throws Exception {
    CreateResponse records = createRecord(indexProvider);
    UpdateRequest request =
        new UpdateRequestImpl(
            records.getCreatedMetacards().stream().map(m -> m.getId()).toArray(String[]::new),
            records.getCreatedMetacards());
    when(catalogProvider.update(request))
        .thenReturn(
            new UpdateResponseImpl(
                request,
                request.getProperties(),
                records.getCreatedMetacards(),
                records.getCreatedMetacards()));
    UpdateResponse response = indexProvider.update(request);
    assertThat(response.getUpdatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testDelete() throws Exception {
    CreateResponse records = createRecord(indexProvider);
    DeleteRequest request =
        new DeleteRequestImpl(
            records.getCreatedMetacards().stream().map(c -> c.getId()).toArray(String[]::new));
    indexProvider.delete(request);
  }

  private CreateResponse createRecord(IndexProvider provider) throws Exception {
    List<Metacard> list =
        Arrays.asList(
            new MockMetacard(Library.getFlagstaffRecord()),
            new MockMetacard(Library.getTampaRecord()));

    CreateRequest request = new CreateRequestImpl(list);
    when(catalogProvider.create(request))
        .thenReturn(new CreateResponseImpl(request, request.getProperties(), list));

    return provider.create(request);
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
