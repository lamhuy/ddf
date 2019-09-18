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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ddf.catalog.data.Metacard;
import ddf.catalog.filter.FilterAdapter;
import ddf.catalog.filter.proxy.adapter.GeotoolsFilterAdapterImpl;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.source.solr.BaseSolrCatalogProvider;
import ddf.catalog.source.solr.SolrFilterDelegateFactory;
import ddf.catalog.source.solr.SolrFilterDelegateFactoryImpl;
import ddf.catalog.source.solr.api.IndexCollectionProvider;
import ddf.catalog.source.solr.api.SolrCollectionCreationPlugin;
import ddf.catalog.source.solr.api.impl.AbstractIndexCollectionProvider;
import ddf.catalog.source.solr.provider.SolrProviderTestUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.MapUtils;
import org.codice.solr.factory.SolrClientFactory;
import org.codice.solr.factory.impl.SolrCloudClientFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrSplitIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrSplitIndex.class);

  private static SolrIndexProvider provider;

  private static List<IndexCollectionProvider> indexCollectionProviders = new ArrayList<>();

  private static List<SolrCollectionCreationPlugin> collectionCreationPlugins = new ArrayList<>();

  private static SolrClientFactory solrClientFactory;

  private static SolrCollectionCreationPlugin creationPlugin =
      mock(SolrCollectionCreationPlugin.class);

  private static final String TEST_COLLECTION = "catalog_test";

  private static final String CATALOG_COLLECTION = "catalog";

  @BeforeClass
  public static void setUp() throws Exception {
    TestIndexCollectionProvider testIndexCollectionProvider = new TestIndexCollectionProvider();
    //    testIndexCollectionProvider.setShardCount(2);
    solrClientFactory = new SolrCloudClientFactory();
    FilterAdapter filterAdapter = new GeotoolsFilterAdapterImpl();
    SolrFilterDelegateFactory filterDelegateFactory = new SolrFilterDelegateFactoryImpl();
    indexCollectionProviders.add(testIndexCollectionProvider);
    collectionCreationPlugins.add(creationPlugin);
    provider =
        new SolrIndexProvider(
            solrClientFactory,
            filterAdapter,
            filterDelegateFactory,
            indexCollectionProviders,
            collectionCreationPlugins);
  }

  @Test
  public void testCreate() throws Exception {
    cleanup();
    CreateRequest createRequest =
        new CreateRequestImpl(MockMetacard.createMetacard(Library.getTampaRecord()));
    assertThat(provider.isAvailable(), is(true));
    CreateResponse response = provider.create(createRequest);
    assertThat(solrClientFactory.collectionExists(TEST_COLLECTION), is(true));
    verify(creationPlugin, times(1)).collectionCreated(any());
  }

  private void cleanup() throws UnsupportedQueryException, IngestException {
    if (provider != null && MapUtils.isNotEmpty(provider.catalogProviders)) {
      for (BaseSolrCatalogProvider catalogProvider : provider.catalogProviders.values()) {
        SolrProviderTestUtil.deleteAll(catalogProvider);
      }
    }

    solrClientFactory.removeCollection(CATALOG_COLLECTION);
  }

  private static class TestIndexCollectionProvider extends AbstractIndexCollectionProvider {

    @Override
    protected String getCollectionName() {
      return TEST_COLLECTION;
    }

    @Override
    protected boolean matches(Metacard metacard) {
      return true;
    }
  }
}
