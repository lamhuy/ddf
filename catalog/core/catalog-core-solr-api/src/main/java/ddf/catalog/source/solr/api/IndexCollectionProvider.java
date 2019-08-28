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
package ddf.catalog.source.solr.api;

import ddf.catalog.data.Metacard;
import org.codice.solr.factory.SolrCollectionConfiguration;

import com.sun.istack.internal.Nullable;

/** Provider to get a index collection name for a given request. */
public interface IndexCollectionProvider {

  /**
   * Gets the core name for a Metacard.
   *
   * @param metacard
   * @return Core name
   */
  String getCollection(@Nullable Metacard metacard);

  String getIndexCollectionValue();

  SolrCollectionConfiguration getConfiguration();
}
