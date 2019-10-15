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
package ddf.catalog.provider.solr.defaultindexcollectionprovider;

import ddf.catalog.data.Metacard;
import ddf.catalog.source.solr.api.impl.AbstractIndexCollectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is the default collection provider and will always return `catalog_index` */
public class DefaultSolrIndexCollectionProvider extends AbstractIndexCollectionProvider {
  static final String DEFAULT_INDEX_COLLECTION = "index";

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultSolrIndexCollectionProvider.class);

  @Override
  protected String getCollectionName() {
    return DEFAULT_INDEX_COLLECTION;
  }

  @Override
  protected boolean matches(Metacard metacard) {
    return true;
  }

  @Override
  public String getCollection(Metacard metacard) {
    LOGGER.trace("Returning Index Collection: {}", DEFAULT_INDEX_COLLECTION);
    return DEFAULT_INDEX_COLLECTION;
  }
}
