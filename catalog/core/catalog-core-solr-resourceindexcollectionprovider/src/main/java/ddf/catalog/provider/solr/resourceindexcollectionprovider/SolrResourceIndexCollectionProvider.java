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
package ddf.catalog.provider.solr.resourceindexcollectionprovider;

import ddf.catalog.data.Attribute;
import ddf.catalog.data.Metacard;
import ddf.catalog.data.types.Core;
import ddf.catalog.source.solr.api.IndexCollectionProvider;
import ddf.catalog.source.solr.api.impl.SolrCollectionConfigurationImpl;
import ddf.catalog.source.solr.api.impl.SolrConfigurationDataImpl;
import ddf.catalog.util.impl.DescribableImpl;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.codice.solr.factory.SolrCollectionConfiguration;
import org.codice.solr.factory.SolrConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is the default collection provider and will always return `catalog_index` */
public class SolrResourceIndexCollectionProvider extends DescribableImpl
    implements IndexCollectionProvider {
  static final String INDEX_COLLECTION = "catalog_resource";

  private static final List<String> SOLR_CONFIG_FILES =
      Collections.unmodifiableList(
          Arrays.asList(
              "dictionary.txt",
              "protwords.txt",
              "schema.xml",
              "solr.xml",
              "solrconfig.xml",
              "stopwords.txt",
              "stopwords_en.txt",
              "synonyms.txt"));

  private int initialShardCount;

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SolrResourceIndexCollectionProvider.class);

  @Override
  public String getCollection(Metacard metacard) {
    Attribute tagAttr = metacard.getAttribute(Core.METACARD_TAGS);
    if (tagAttr != null) {
      for (Serializable attr : tagAttr.getValues()) {
        if (attr instanceof String) {
          String tag = (String) attr;
          if (tag.equalsIgnoreCase("resource")) {
            LOGGER.trace("Returning Index Collection: {}", INDEX_COLLECTION);
            return INDEX_COLLECTION;
          }
        }
      }
    }
    return null;
  }

  @Override
  public SolrCollectionConfiguration getConfiguration() {
    List<SolrConfigurationData> configurationData = new ArrayList<>(SOLR_CONFIG_FILES.size());
    for (String filename : SOLR_CONFIG_FILES) {
      InputStream inputStream =
          SolrResourceIndexCollectionProvider.class
              .getClassLoader()
              .getResourceAsStream("solr/conf/" + filename);
      SolrConfigurationData solrConfigurationDataFile =
          new SolrConfigurationDataImpl(filename, inputStream);
      configurationData.add(solrConfigurationDataFile);
    }
    return new SolrCollectionConfigurationImpl(
        INDEX_COLLECTION, initialShardCount, configurationData);
  }

  public void setInitialShardCount(int initialShardCount) {
    this.initialShardCount = initialShardCount;
  }
}
