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
package ddf.catalog.operation.impl;

import ddf.catalog.operation.IndexDeleteResponse;
import ddf.catalog.operation.Request;
import ddf.catalog.operation.SourceProcessingDetails;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The SourceResponseImpl represents a default implementation of the {@link IndexDeleteResponse}.
 */
public class IndexDeleteResponseImpl extends ResponseImpl<Request> implements IndexDeleteResponse {

  protected long hits;

  protected Set<SourceProcessingDetails> sourceProcessingDetails = null;

  ResponseImpl<Request> deleteResponse;

  private Map<Set<String>, Set<String>> taggedIds = new HashMap<>();

  /**
   * Instantiates a new SourceResponseImpl with the original query request and results from the
   * query being executed.
   *
   * @param request the original request
   */
  public IndexDeleteResponseImpl(Request request) {
    this(request, null, 0);
  }

  /**
   * Instantiates a new SourceResponseImpl. Use when the total amount of hits is known.
   *
   * @param request the original request
   * @param totalHits the total results associated with the query.
   */
  public IndexDeleteResponseImpl(Request request, Long totalHits) {
    this(request, null, totalHits != null ? totalHits.longValue() : 0);
  }

  /**
   * Instantiates a new SourceResponseImpl with properties.
   *
   * @param request the original request
   * @param properties the properties associated with the operation
   */
  public IndexDeleteResponseImpl(Request request, Map<String, Serializable> properties) {
    this(request, properties, 0);
  }

  /**
   * Instantiates a new SourceResponseImpl with properties and when the total number of hits is
   * known.
   *
   * @param request the original request
   * @param properties the properties associated with the operation
   * @param totalHits the total hits
   */
  public IndexDeleteResponseImpl(
      Request request, Map<String, Serializable> properties, long totalHits) {
    super(request, properties);
    deleteResponse = new ResponseImpl<>(request, properties);
    this.hits = totalHits;
  }

  /*
   * (non-Javadoc)
   *
   * @see ddf.catalog.operation.OperationImpl#containsPropertyName(java.lang.String)
   */
  public boolean containsPropertyName(String name) {
    return deleteResponse.containsPropertyName(name);
  }

  /*
   * (non-Javadoc)
   *
   * @see ddf.catalog.operation.SourceResponse#getHits()
   */
  @Override
  public long getHits() {
    return hits;
  }

  /**
   * Sets the hits.
   *
   * @param hits the new hits
   */
  public void setHits(long hits) {
    this.hits = hits;
  }

  /*
   * (non-Javadoc)
   *
   * @see ddf.catalog.operation.SourceResponse#getResults()
   */
  @Override
  public Set<String> getIds(Set<String> tags) {
    return taggedIds.get(tags);
  }

  @Override
  public Set<Set<String>> getTags() {
    return taggedIds.keySet();
  }

  /**
   * Build a list of metacard id of the same tag. Note: a metacard could have multiple tags, tag
   * order does matter
   *
   * @param tags
   * @param id
   */
  public void addTaggedId(Set<String> tags, String id) {
    taggedIds.computeIfPresent(
        tags,
        (key, value) -> {
          value.add(id);
          return value;
        });
    taggedIds.computeIfAbsent(tags, k -> new HashSet<>(Arrays.asList(id)));
  }

  /*
   * (non-Javadoc)
   *
   * @see ddf.catalog.operation.SourceResponse#getProcessingDetails()
   */
  @Override
  public Set<SourceProcessingDetails> getProcessingDetails() {
    return sourceProcessingDetails;
  }

  /**
   * Sets the warnings associated with the {@link Source}.
   *
   * @param warnings the new warnings associated with the {@link Source}.
   */
  public void setWarnings(List<String> warnings) {
    if (warnings != null && !warnings.isEmpty()) {
      sourceProcessingDetails = new HashSet<>();
      sourceProcessingDetails.add(new SourceProcessingDetailsImpl(warnings));
    }
  }
}
