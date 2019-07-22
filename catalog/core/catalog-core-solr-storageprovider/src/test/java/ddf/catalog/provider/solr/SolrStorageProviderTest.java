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
import ddf.catalog.data.impl.ResultImpl;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.CreateResponseImpl;
import ddf.catalog.operation.impl.DeleteRequestImpl;
import ddf.catalog.operation.impl.QueryImpl;
import ddf.catalog.operation.impl.QueryRequestImpl;
import ddf.catalog.operation.impl.SourceResponseImpl;
import ddf.catalog.operation.impl.UpdateRequestImpl;
import ddf.catalog.operation.impl.UpdateResponseImpl;
import ddf.catalog.source.StorageProvider;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.DynamicSchemaResolver;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.codice.solr.factory.impl.HttpClientBuilder;
import org.codice.solr.factory.impl.HttpSolrClientFactory;
import org.junit.Before;
import org.junit.Test;
import org.opengis.filter.Filter;

public class SolrStorageProviderTest extends SolrStorageProvider {

  private static SolrFilterBuilder filterBuilder = new SolrFilterBuilder();
  private SolrStorageProvider storageProvider;

  public SolrStorageProviderTest() {
    super(
        new HttpSolrClientFactory(mock(HttpClientBuilder.class)),
        mock(FilterAdapter.class),
        mock(SolrFilterDelegateFactory.class),
        mock(DynamicSchemaResolver.class));

    provider = mock(BaseSolrCatalogProvider.class);
  }

  @Before
  public void setup() {
    storageProvider = this;
  }

  @Test
  public void testCreate() throws Exception {
    CreateResponse response = createRecord(storageProvider);
    assertThat(response.getCreatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testQuery() throws Exception {

    CreateResponse records = createRecord(storageProvider);
    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));

    when(provider.query(request))
        .thenReturn(
            new SourceResponseImpl(
                request,
                records
                    .getCreatedMetacards()
                    .stream()
                    .map(c -> new ResultImpl(c))
                    .collect(Collectors.toList()),
                2L));

    SourceResponse response = storageProvider.query(request);
    assertThat(response.getHits(), equalTo(2L));
  }

  @Test
  public void testQueryById() throws Exception {

    CreateResponse records = createRecord(storageProvider);
    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));

    when(provider.query(request))
        .thenReturn(
            new SourceResponseImpl(
                request,
                records
                    .getCreatedMetacards()
                    .stream()
                    .map(c -> new ResultImpl(c))
                    .collect(Collectors.toList()),
                2L));

    SourceResponse response =
        storageProvider.queryByIds(
            request,
            records
                .getCreatedMetacards()
                .stream()
                .map(m -> m.getId())
                .collect(Collectors.toList()));
    assertThat(response.getHits(), equalTo(2L));
  }

  @Test
  public void testUpdate() throws Exception {
    CreateResponse records = createRecord(storageProvider);
    UpdateRequest request =
        new UpdateRequestImpl(
            records.getCreatedMetacards().stream().map(m -> m.getId()).toArray(String[]::new),
            records.getCreatedMetacards());
    when(provider.update(request))
        .thenReturn(
            new UpdateResponseImpl(
                request,
                request.getProperties(),
                records.getCreatedMetacards(),
                records.getCreatedMetacards()));
    UpdateResponse response = storageProvider.update(request);
    assertThat(response.getUpdatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testDelete() throws Exception {
    CreateResponse records = createRecord(storageProvider);
    DeleteRequest request =
        new DeleteRequestImpl(
            records.getCreatedMetacards().stream().map(c -> c.getId()).toArray(String[]::new));
    storageProvider.delete(request);
  }

  private CreateResponse createRecord(StorageProvider storageProvider) throws Exception {
    List<Metacard> list =
        Arrays.asList(
            new MockMetacard(Library.getFlagstaffRecord()),
            new MockMetacard(Library.getTampaRecord()));

    CreateRequest request = new CreateRequestImpl(list);
    when(this.provider.create(request))
        .thenReturn(new CreateResponseImpl(request, request.getProperties(), list));

    return storageProvider.create(request);
  }

  @Test
  public void testShutdown() {
    storageProvider.shutdown();
  }

  @Test
  public void testIsAvailable() {
    storageProvider.isAvailable();
    when(provider.isAvailable()).thenReturn(true);
    when(provider.isAvailable(any())).thenReturn(true);
    storageProvider.isAvailable(null);
  }

  @Test
  public void testGetContentTypes() {
    storageProvider.getContentTypes();
  }

  @Test
  public void testMaskId() {
    storageProvider.maskId("id");
  }
}
