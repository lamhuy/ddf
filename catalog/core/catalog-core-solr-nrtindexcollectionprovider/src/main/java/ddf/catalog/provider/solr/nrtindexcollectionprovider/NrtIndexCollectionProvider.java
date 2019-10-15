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
package ddf.catalog.provider.solr.nrtindexcollectionprovider;

import ddf.catalog.data.Attribute;
import ddf.catalog.data.Metacard;
import ddf.catalog.data.types.Core;
import ddf.catalog.source.solr.api.impl.AbstractIndexCollectionProvider;
import java.io.Serializable;
import java.util.List;
import org.springframework.util.CollectionUtils;

public class NrtIndexCollectionProvider extends AbstractIndexCollectionProvider {
  static final String COLLECTION = "nrt";

  private List<String> nrtMetacardTypes;

  public void setNrtMetacardTypes(List<String> nrtMetacardTypes) {
    this.nrtMetacardTypes = nrtMetacardTypes;
  }

  @Override
  protected String getCollectionName() {
    return COLLECTION;
  }

  @Override
  protected boolean matches(Metacard metacard) {
    if (CollectionUtils.isEmpty(nrtMetacardTypes)) {
      return false;
    }

    Attribute tagAttr = metacard.getAttribute(Core.METACARD_TAGS);
    if (tagAttr != null) {
      for (Serializable attr : tagAttr.getValues()) {
        if (attr instanceof String) {
          String tag = (String) attr;
          if (nrtMetacardTypes.contains(tag)) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
