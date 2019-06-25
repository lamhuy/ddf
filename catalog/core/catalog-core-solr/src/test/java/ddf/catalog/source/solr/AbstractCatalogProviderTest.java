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

import static ddf.catalog.source.solr.DynamicSchemaResolver.FIVE_MEGABYTES;

import ddf.catalog.filter.proxy.adapter.GeotoolsFilterAdapterImpl;
import java.nio.file.Paths;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.codice.solr.factory.impl.SolrCloudClientFactory;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  //  SolrProviderContentTypes.class,
  //  SolrProviderCreate.class,
  //  SolrProviderDelete.class,
  //  SolrProviderExtensibleMetacards.class,
  //  SolrProviderQuery.class,
  //  SolrProviderSorting.class,
  //  SolrProviderSource.class,
  //  SolrProviderSpatial.class,
  //  SolrProviderTemporal.class,
  //  SolrProviderUpdate.class,
  //  SolrProviderXpath.class
})
public class AbstractCatalogProviderTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCatalogProviderTest.class);

  @Rule @ClassRule public static TemporaryFolder baseDir = new TemporaryFolder();

  private static SolrIndexProvider indexProvider = null;
  private static SolrStorageProvider storageProvider = null;

  private static MiniSolrCloudCluster miniSolrCloud;

  public static final String MASKED_ID = "scp";

  @BeforeClass
  public static void beforeClass() throws Exception {
    ConfigurationStore store = ConfigurationStore.getInstance();
    store.setForceAutoCommit(true);
    String solrDataPath = Paths.get("target/surefire/solr").toString();
    System.setProperty("solr.data.dir", solrDataPath);
    store.setDataDirectoryPath(solrDataPath);

    miniSolrCloud =
        new MiniSolrCloudCluster(
            1, baseDir.getRoot().toPath(), JettyConfig.builder().setContext("/solr").build());

    System.setProperty("solr.cloud.shardCount", "1");
    System.setProperty("solr.cloud.replicationFactor", "1");
    System.setProperty("solr.cloud.maxShardPerNode", "1");
    System.setProperty("solr.cloud.zookeeper.chroot", "/solr");
    System.setProperty("solr.cloud.zookeeper", miniSolrCloud.getZkServer().getZkHost());
    System.setProperty("metadata.size.limit", Integer.toString(FIVE_MEGABYTES));

    SolrCloudClientFactory solrClientFactory = new SolrCloudClientFactory();

    indexProvider =
        new SolrIndexProvider(
            solrClientFactory,
            new GeotoolsFilterAdapterImpl(),
            new SolrFilterDelegateFactoryImpl());
    storageProvider =
        new SolrStorageProvider(
            solrClientFactory,
            new GeotoolsFilterAdapterImpl(),
            new SolrFilterDelegateFactoryImpl());

    // Mask the id, this is something that the CatalogFramework would usually do
    indexProvider.maskId(MASKED_ID);
    storageProvider.maskId(MASKED_ID);

    Assert.assertThat(indexProvider.isAvailable(), Matchers.equalTo(true));
    Assert.assertThat(storageProvider.isAvailable(), Matchers.equalTo(true));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solr.data.dir");
    System.clearProperty("solr.cloud.shardCount");
    System.clearProperty("solr.cloud.replicationFactor");
    System.clearProperty("solr.cloud.maxShardPerNode");
    System.clearProperty("solr.cloud.zookeeper.chroot");
    System.clearProperty("solr.cloud.zookeeper");
    System.clearProperty("metadata.size.limit");

    if (miniSolrCloud != null) {
      miniSolrCloud.shutdown();
    }

    indexProvider.shutdown();
    storageProvider.shutdown();
  }
}
