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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ddf.catalog.data.Metacard;
import ddf.catalog.filter.FilterAdapter;
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
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.filter.Filter;

public class SolrIndexProviderTest extends SolrIndexProvider {

  private static SolrFilterBuilder filterBuilder = new SolrFilterBuilder();
  private BaseSolrCatalogProvider catalogProvider;
  private SolrIndexProvider indexProvider;

  public SolrIndexProviderTest() {
    super(
        mock(SolrClientFactory.class),
        mock(FilterAdapter.class),
        mock(SolrFilterDelegateFactory.class),
        mock(DynamicSchemaResolver.class),
        Collections.emptyList(),
        Collections.emptyList());
  }

  @Before
  public void setup() {
    indexProvider = this;
  }

  @Override
  protected BaseSolrCatalogProvider newProvider(String core) {
    when(clientFactory.newClient(anyString())).thenReturn(mock(SolrClient.class));
    super.newProvider(core);
    catalogProvider = mock(BaseSolrCatalogProvider.class);
    return catalogProvider;
  }

  @Test
  public void testCreate() throws Exception {
    CreateResponse response = createRecord(indexProvider);
    assertThat(response.getCreatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testQuery() throws Exception {

    CreateResponse records = createRecord(indexProvider);
    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));
    when(catalogProvider.queryIndex(request))
        .thenReturn(
            new IndexQueryResponseImpl(
                request,
                records
                    .getCreatedMetacards()
                    .stream()
                    .map(c -> c.getId())
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
  @Ignore
  public void testGetCatalogProvider() throws Exception {
    when(clientFactory.isSolrCloud()).thenReturn(true);

    List<Metacard> list =
        Arrays.asList(
            new MockMetacard(Library.getFlagstaffRecord()),
            new MockMetacard(Library.getTampaRecord()));
    CreateRequest createRequest = new CreateRequestImpl(list);
    UpdateRequest updateRequest = new UpdateRequestImpl(list.get(0).getId(), list.get(0));
    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    QueryRequest queryRequest = new QueryRequestImpl(new QueryImpl(filter));
    // TODO Fix test
    //    assertThat(indexProvider.create(createRequest), is(catalogProvider));
    //    assertThat(indexProvider.update(updateRequest), is(catalogProvider));
    //    assertThat(indexProvider.query(queryRequest), is(catalogProvider));
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
