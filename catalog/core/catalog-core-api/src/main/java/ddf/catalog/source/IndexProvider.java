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
package ddf.catalog.source;

import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.IndexQueryResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.util.Maskable;

/**
 * External facing (outside of {@link ddf.catalog.CatalogFramework}) API used to interact with the
 * index provider in a file system or database. The basic premise of a CatalogProvider is to allow
 * query, create, update, and delete operations of the indexed.
 *
 * <p>The provider performs a look up of the collection to maintain the index data. The key
 * functions of the CatalogProvider can be found in the {@link Source}.
 */
public interface IndexProvider extends Maskable {

  /** Publishes a list of id into the catalog. */
  public CreateResponse create(CreateRequest createRequest) throws IngestException;

  /** Updates a list of ids. Ids that are not in the Catalog will not be created. * */
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException;

  /**
   * Delete the indexes for a given list if metacard ids
   *
   * @param deleteRequest
   * @return a mapping of metacard type with the list of associate ids
   * @throws IngestException
   */
  public void delete(DeleteRequest deleteRequest) throws IngestException;

  public IndexQueryResponse query(QueryRequest queryRequest) throws UnsupportedQueryException;

  public void shutdown();
}
