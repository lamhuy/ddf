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
package org.codice.solr.factory.impl;

import static org.apache.commons.lang.Validate.notNull;

import com.google.common.annotations.VisibleForTesting;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import org.codice.solr.client.solrj.SolrClient;
import org.codice.solr.factory.SolrClientFactory;
import org.codice.solr.factory.SolrConfigurationData;

/**
 * Factory class used to create the proper {@link SolrClient} based on the current {@code
 * solr.client} system property.
 */
public final class SolrClientFactoryImpl implements SolrClientFactory {

  private String clientType;
  private SolrClientFactory factory;

  public SolrClientFactoryImpl(HttpSolrClientFactory httpSolrClientFactory) {
    this.clientType =
        AccessController.doPrivileged(
            (PrivilegedAction<String>) () -> System.getProperty("solr.client", "HttpSolrClient"));

    if ("CloudSolrClient".equals(clientType)) {
      factory = new SolrCloudClientFactory();
    } else { // Use HttpSolrClient by default
      factory = httpSolrClientFactory;
    }
  }

  @Override
  public SolrClient newClient(String core) {
    notNull(core, "Solr core name cannot be null");
    return factory.newClient(core);
  }

  @Override
  public SolrClient getClient(String core) {
    return factory.getClient(core);
  }

  @Override
  public boolean isSolrCloud() {
    return false;
  }

  @Override
  public boolean collectionExists(String collection) {
    return factory.collectionExists(collection);
  }

  @Override
  public void addConfiguration(
      String configurationName, List<SolrConfigurationData> configurationData) {
    factory.addConfiguration(configurationName, configurationData);
  }

  @Override
  public void addCollection(
      String collection, Integer shardCountRequested, String configurationName) {
    factory.addCollection(collection, shardCountRequested, configurationName);
  }

  @Override
  public void addCollectionToAlias(String alias, String collection) {
    factory.addCollectionToAlias(alias, collection);
  }

  @VisibleForTesting
  SolrClientFactory getFactory() {
    return factory;
  }
}
