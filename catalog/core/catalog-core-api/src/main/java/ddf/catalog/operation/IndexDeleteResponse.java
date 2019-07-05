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
package ddf.catalog.operation;

import java.util.Set;

/**
 * Query status should come back in the properties.
 *
 * @author Michael Menousek
 */
public interface IndexDeleteResponse extends Response<Request> {

  /**
   * The total number of hits matching the associated {@link Query} for the associated {@link
   * ddf.catalog.source.Source}, -1 if unknown. This is typically more than the number of indexes
   * are returned from {@link #getIds(Set<String>)}.
   *
   * @return long - total hits matching this {@link Query}, -1 if unknown.
   */
  public long getHits();

  /**
   * Get the list of deleted index id associate with a given tag for {@link Request}
   *
   * @return the index id
   */
  public Set<String> getIds(Set<String> tag);

  /**
   * Get list of tags for all deleted index for {@link Request}
   *
   * @return
   */
  public Set<Set<String>> getTags();
  /**
   * Get any specific details about the execution of the {@link Request} associated with this {@link
   * IndexDeleteResponse}. <b>Must not be null.</b>
   *
   * @return the processing details
   */
  public void addTaggedId(Set<String> tags, String id);

  public Set<? extends SourceProcessingDetails> getProcessingDetails();
}
