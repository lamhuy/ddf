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
package ddf.catalog.source.solr.metacardtypecache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import ddf.catalog.data.MetacardType;
import ddf.catalog.source.MetacardTypeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetacardTypeCacheImpl implements MetacardTypeCache {

  protected Cache<String, MetacardType> metacardTypesCache =
      CacheBuilder.newBuilder().maximumSize(4096).initialCapacity(64).build();

  protected Cache<String, byte[]> metacardTypeNameToSerialCache =
      CacheBuilder.newBuilder().maximumSize(4096).initialCapacity(64).build();

  private static final Logger LOGGER = LoggerFactory.getLogger(MetacardTypeCacheImpl.class);

  public MetacardTypeCacheImpl() {}

  @Override
  public MetacardType getMetacardType(String type) {
    return metacardTypesCache.getIfPresent(type);
  }

  @Override
  public void addMetacardType(String type, MetacardType metacardType) {
    metacardTypesCache.put(type, metacardType);
  }

  @Override
  public void addMetacardSerializedType(String type, byte[] metacardTypeBytes) {
    metacardTypeNameToSerialCache.put(type, metacardTypeBytes);
  }

  @Override
  public byte[] getMetacardSerializedType(String type) {
    return metacardTypeNameToSerialCache.getIfPresent(type);
  }

  public void setCacheSize(int cacheSize) {
    metacardTypesCache = CacheBuilder.newBuilder().maximumSize(4096).initialCapacity(64).build();
    metacardTypeNameToSerialCache =
        CacheBuilder.newBuilder().maximumSize(4096).initialCapacity(64).build();
  }
}
