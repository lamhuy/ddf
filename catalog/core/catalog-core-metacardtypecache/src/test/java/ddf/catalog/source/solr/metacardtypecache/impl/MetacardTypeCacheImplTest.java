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
package ddf.catalog.source.solr.metacardtypecache.impl;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import ddf.catalog.data.MetacardType;
import ddf.catalog.data.impl.MetacardTypeImpl;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;

public class MetacardTypeCacheImplTest {

  private MetacardTypeCacheImpl metacardTypeCache;

  private MetacardType testType1 = new MetacardTypeImpl("type1", Collections.emptySet());

  private MetacardType testType2 = new MetacardTypeImpl("type2", Collections.emptySet());

  @Before
  public void setUp() {
    metacardTypeCache = new MetacardTypeCacheImpl();
  }

  @Test
  public void testGetMetacardType() {
    metacardTypeCache.addMetacardType(testType1.getName(), testType1);
    MetacardType returnType = metacardTypeCache.getMetacardType(testType1.getName());
    assertThat(returnType.getName(), is(testType1.getName()));
  }

  @Test
  public void testGetMetacardTypeBytes() {
    metacardTypeCache.addMetacardSerializedType(
        testType1.getName(), testType1.getName().getBytes());
    byte[] cacheBytes = metacardTypeCache.getMetacardSerializedType(testType1.getName());
    String cacheString = new String(cacheBytes);
    assertThat(cacheString, is(testType1.getName()));
  }

  @Test
  public void testCacheSize() {
    metacardTypeCache.setCacheSize(1);
    metacardTypeCache.addMetacardType(testType1.getName(), testType1);
    metacardTypeCache.addMetacardType(testType2.getName(), testType2);
    MetacardType returnType1 = metacardTypeCache.getMetacardType(testType1.getName());
    MetacardType returnType2 = metacardTypeCache.getMetacardType(testType2.getName());
    assertThat(returnType2.getName(), is(testType2.getName()));
    assertThat(returnType1, is(nullValue()));
  }

  @Test
  public void testIsTypeCached() {
    metacardTypeCache.setCacheSize(1);
    metacardTypeCache.addMetacardType(testType1.getName(), testType1);
    metacardTypeCache.addMetacardType(testType2.getName(), testType2);
    assertThat(metacardTypeCache.isTypeCached(testType1.getName()), is(false));
    assertThat(metacardTypeCache.isTypeCached(testType2.getName()), is(true));
  }

  @Test
  public void testGetIndexName() {
    String indexName = metacardTypeCache.getIndexName(testType1);
    assertThat(indexName, startsWith(testType1.getName()));
  }
}
