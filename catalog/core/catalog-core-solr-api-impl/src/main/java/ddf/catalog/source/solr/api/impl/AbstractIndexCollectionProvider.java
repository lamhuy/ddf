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
package ddf.catalog.source.solr.api.impl;

import ddf.catalog.data.Metacard;
import ddf.catalog.source.solr.api.IndexCollectionProvider;
import ddf.catalog.util.impl.DescribableImpl;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.codice.solr.factory.SolrCollectionConfiguration;
import org.codice.solr.factory.SolrConfigurationData;

public abstract class AbstractIndexCollectionProvider extends DescribableImpl
    implements IndexCollectionProvider {
  static final List<String> SOLR_CONFIG_FILES =
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

  private int shardCount;

  protected abstract String getCollectionName();

  protected abstract boolean matches(Metacard metacard);

  @Override
  public String getCollection(Metacard metacard) {
    if (metacard == null) {
      return null;
    }

    if (matches(metacard)) {
      return getCollectionName();
    }

    return null;
  }

  @Override
  public SolrCollectionConfiguration getConfiguration() {
    List<SolrConfigurationData> configurationData = new ArrayList<>(SOLR_CONFIG_FILES.size());
    for (String filename : SOLR_CONFIG_FILES) {
      InputStream inputStream =
          getClass().getClassLoader().getResourceAsStream("solr/conf/" + filename);
      if (inputStream == null) {
        inputStream =
            getClass().getClassLoader().getResourceAsStream("solr-schema/solr/conf/" + filename);
      }
      if (inputStream != null) {
        SolrConfigurationData solrConfigurationDataFile =
            new SolrConfigurationDataImpl(filename, inputStream);
        configurationData.add(solrConfigurationDataFile);
      }
    }
    return new SolrCollectionConfigurationImpl(getCollectionName(), shardCount, configurationData);
  }

  public void setShardCount(int shardCount) {
    this.shardCount = shardCount;
  }
}
