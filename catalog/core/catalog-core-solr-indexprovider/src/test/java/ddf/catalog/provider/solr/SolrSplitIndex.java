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
import static org.hamcrest.Matchers.is;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.impl.AttributeImpl;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.filter.FilterBuilder;
import ddf.catalog.filter.proxy.adapter.GeotoolsFilterAdapterImpl;
import ddf.catalog.filter.proxy.builder.GeotoolsFilterBuilder;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.IndexQueryResponse;
import ddf.catalog.operation.Query;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.DeleteRequestImpl;
import ddf.catalog.operation.impl.QueryImpl;
import ddf.catalog.operation.impl.QueryRequestImpl;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import ddf.catalog.source.solr.SolrFilterDelegateFactoryImpl;
import ddf.catalog.source.solr.api.IndexCollectionProvider;
import ddf.catalog.source.solr.api.SolrCollectionCreationPlugin;
import ddf.catalog.source.solr.api.impl.AbstractIndexCollectionProvider;
import java.util.ArrayList;
import java.util.List;
import org.codice.solr.factory.SolrClientFactory;
import org.codice.solr.factory.impl.SolrCloudClientFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrSplitIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrSplitIndex.class);

  private static SolrIndexProvider provider;

  private static List<IndexCollectionProvider> indexCollectionProviders = new ArrayList<>();

  private static List<SolrCollectionCreationPlugin> collectionCreationPlugins = new ArrayList<>();

  private static SolrClientFactory solrClientFactory;

  private static final String TEST_COLLECTION = "catalog_test";

  private static final String TEST_ALT_COLLECTION = "catalog_alt";

  private static final String CATALOG_ALIAS = "catalog";

  private static final String SIMPLE_CONTENT_TYPE = "simple";

  private static final String ALT_CONTENT_TYPE = "alternate";

  private FilterBuilder filterBuilder;

  @BeforeClass
  public static void setUpClass() {
    TestIndexCollectionProvider testIndexCollectionProvider = new TestIndexCollectionProvider();
    testIndexCollectionProvider.setShardCount(1);

    TestAlternateCollectionProvider testAlternateCollectionProvider =
        new TestAlternateCollectionProvider();
    testAlternateCollectionProvider.setShardCount(1);

    solrClientFactory = new SolrCloudClientFactory();

    FilterAdapter filterAdapter = new GeotoolsFilterAdapterImpl();
    SolrFilterDelegateFactory filterDelegateFactory = new SolrFilterDelegateFactoryImpl();
    indexCollectionProviders.add(testIndexCollectionProvider);
    indexCollectionProviders.add(testAlternateCollectionProvider);
    provider =
        new SolrIndexProvider(
            solrClientFactory,
            filterAdapter,
            filterDelegateFactory,
            indexCollectionProviders,
            collectionCreationPlugins);
    provider.setForceAutoCommit(true);
  }

  @Before
  public void setUp() {
    filterBuilder = new GeotoolsFilterBuilder();
  }

  @Test
  public void testCreate() throws Exception {
    assertThat(provider.isAvailable(), is(true));
    Metacard metacard = MockMetacard.createMetacard(Library.getTampaRecord());
    metacard.setAttribute(new AttributeImpl(Metacard.CONTENT_TYPE, SIMPLE_CONTENT_TYPE));
    CreateRequest createRequest = new CreateRequestImpl(metacard);
    CreateResponse response = provider.create(createRequest);
    assertThat(solrClientFactory.collectionExists(TEST_COLLECTION), is(true));

    assertIdExists(metacard.getId());
    deleteAndValidate(metacard.getId());
  }

  @Test
  public void testCreateMultipleCollections() throws Exception {
    assertThat(provider.isAvailable(), is(true));
    Metacard metacardSimple = MockMetacard.createMetacard(Library.getTampaRecord());
    metacardSimple.setAttribute(new AttributeImpl(Metacard.CONTENT_TYPE, SIMPLE_CONTENT_TYPE));
    Metacard metacardAlt = MockMetacard.createMetacard(Library.getFlagstaffRecord());
    metacardAlt.setAttribute(new AttributeImpl(Metacard.CONTENT_TYPE, ALT_CONTENT_TYPE));
    assertThat(provider.isAvailable(), is(true));

    CreateRequest createRequest;

    createRequest = new CreateRequestImpl(metacardSimple);
    provider.create(createRequest);
    createRequest = new CreateRequestImpl(metacardAlt);
    provider.create(createRequest);

    assertThat(solrClientFactory.collectionExists(TEST_COLLECTION), is(true));
    assertThat(solrClientFactory.collectionExists(TEST_ALT_COLLECTION), is(true));

    assertIdExists(metacardSimple.getId());
    assertIdExists(metacardAlt.getId());

    deleteAndValidate(metacardSimple.getId());
    deleteAndValidate(metacardAlt.getId());
  }

  private void assertIdExists(String metacardId) throws UnsupportedQueryException {
    IndexQueryResponse queryResponse = getId(metacardId);
    assertThat(queryResponse.getIds().iterator().next(), is(metacardId));
  }

  private void deleteAndValidate(String id) throws IngestException, UnsupportedQueryException {
    DeleteRequest deleteRequest = new DeleteRequestImpl(id);
    provider.delete(deleteRequest);

    IndexQueryResponse response = getId(id);
    assertThat(response.getHits(), is(0L));
  }

  private IndexQueryResponse getId(String metacardId) throws UnsupportedQueryException {
    Filter queryFilter = filterBuilder.attribute(Metacard.ID).is().equalTo().text(metacardId);
    Query query = new QueryImpl(queryFilter);
    QueryRequest queryRequest = new QueryRequestImpl(query);
    return provider.query(queryRequest);
  }

  private static class TestIndexCollectionProvider extends AbstractIndexCollectionProvider {

    @Override
    protected String getCollectionName() {
      return TEST_COLLECTION;
    }

    @Override
    protected boolean matches(Metacard metacard) {
      return metacard.getContentTypeName().equals(SIMPLE_CONTENT_TYPE);
    }
  }

  private static class TestAlternateCollectionProvider extends AbstractIndexCollectionProvider {

    @Override
    protected String getCollectionName() {
      return TEST_ALT_COLLECTION;
    }

    @Override
    protected boolean matches(Metacard metacard) {
      return metacard.getContentTypeName().equals(ALT_CONTENT_TYPE);
    }
  }
}
