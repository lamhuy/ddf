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
package ddf.catalog.provider.solr;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.MetacardType;
import ddf.catalog.data.impl.MetacardImpl;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class MockMetacard {

  public static final String DEFAULT_TITLE = "Flagstaff";

  public static final String DEFAULT_VERSION = "mockVersion";

  public static final String DEFAULT_TYPE = "simple";

  public static final String DEFAULT_LOCATION = "POINT (1 0)";

  public static final String DEFAULT_TAG = "resource";

  public static final byte[] DEFAULT_THUMBNAIL = {-86};

  private static final long serialVersionUID = -189776439741244547L;

  public static Metacard createMetacard(String metadata, MetacardType type, Calendar calendar) {
    MetacardImpl metacard = new MetacardImpl(type);

    // make a simple metacard
    metacard.setCreatedDate(calendar.getTime());
    metacard.setEffectiveDate(calendar.getTime());
    metacard.setExpirationDate(calendar.getTime());
    metacard.setModifiedDate(calendar.getTime());
    metacard.setMetadata(metadata);
    metacard.setContentTypeName(DEFAULT_TYPE);
    metacard.setContentTypeVersion(DEFAULT_VERSION);
    metacard.setLocation(DEFAULT_LOCATION);
    metacard.setThumbnail(DEFAULT_THUMBNAIL);
    metacard.setTitle(DEFAULT_TITLE);
    metacard.setSecurity(new HashMap<>());
    metacard.setTags(Collections.singleton(DEFAULT_TAG));
    return metacard;
  }

  public static Metacard createMetacard(String metadata, MetacardType type) {
    return MockMetacard.createMetacard(metadata, type, Calendar.getInstance());
  }

  public static Metacard createMetacard(String metadata) {
    return MockMetacard.createMetacard(metadata, MetacardImpl.BASIC_METACARD);
  }

  public static List<String> toStringList(List<Metacard> cards) {

    ArrayList<String> stringList = new ArrayList<>();

    for (Metacard m : cards) {
      stringList.add(m.getId());
    }

    return stringList;
  }
}
