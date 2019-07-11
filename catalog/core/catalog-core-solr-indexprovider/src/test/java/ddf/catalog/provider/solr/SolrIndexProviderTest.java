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
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ddf.catalog.data.Metacard;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.IndexQueryResponse;
import ddf.catalog.operation.Query;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.QueryImpl;
import ddf.catalog.operation.impl.QueryRequestImpl;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IndexProvider;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.codice.solr.client.solrj.SolrClient;
import org.codice.solr.factory.SolrClientFactory;
import org.junit.Before;
import org.junit.Test;
import org.opengis.filter.Filter;

public class SolrIndexProviderTest {

  protected final SolrClientFactory clientFactory = mock(SolrClientFactory.class);
  protected final FilterAdapter filterAdapter = mock(FilterAdapter.class);
  protected final SolrFilterDelegateFactory solrFilterDelegateFactory =
      mock(SolrFilterDelegateFactory.class);
  protected final DynamicSchemaResolver resolver = mock(DynamicSchemaResolver.class);
  protected final SolrClient solrClient = mock(SolrClient.class);
  private static SolrFilterBuilder filterBuilder = new SolrFilterBuilder();
  private SolrIndexProvider indexProvider;

  @Before
  public void setup() {
    when(clientFactory.getClient(anyString())).thenReturn(solrClient);
    indexProvider =
        new SolrIndexProvider(clientFactory, filterAdapter, solrFilterDelegateFactory, resolver);
    List<String> parameters = new ArrayList<>();
    parameters.add("resource=catalogIndex");
    indexProvider.setParameters(parameters);
  }

  @Test
  public void testCreate() throws Exception {
    CreateResponse response =  createRecord(indexProvider);

    assertThat(response.getCreatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testQuery() throws Exception {
    createRecord(indexProvider);
    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));
    //IndexQueryResponse response = indexProvider.query(request);

    //assertThat(response.getHits(), equalTo(2));

  }

  private CreateResponse createRecord(IndexProvider provider) throws Exception{
    when(solrClient.add(anyList(), anyInt())).thenReturn(mock(UpdateResponse.class));
    when(solrClient.add(anyList())).thenReturn(mock(UpdateResponse.class));

    List<Metacard> list =
        Arrays.asList(
            new MockMetacard(Library.getFlagstaffRecord()),
            new MockMetacard(Library.getTampaRecord()));

    CreateRequest request = new CreateRequestImpl(list);
    return provider.create(request);
  }

}
