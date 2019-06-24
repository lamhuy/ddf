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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ddf.catalog.data.Attribute;
import ddf.catalog.data.Metacard;
import ddf.catalog.data.types.Core;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;

public class NrtIndexCollectionProviderTest {

  private NrtIndexCollectionProvider indexProvider = new NrtIndexCollectionProvider();

  @Before
  public void setUp() {
    indexProvider.setNrtMetacardTypes(Collections.singletonList("workspace"));
  }

  @Test
  public void testGetCollection() throws Exception {
    Metacard mockCard = getMockMetacard("workspace");
    String collection = indexProvider.getCollection(mockCard);
    assertThat(collection, is(NrtIndexCollectionProvider.COLLECTION));
  }

  @Test
  public void testUnsetNrtTypes() {
    indexProvider.setNrtMetacardTypes(Collections.emptyList());
    Metacard mockCard = getMockMetacard("workspace");
    String collection = indexProvider.getCollection(mockCard);
    assertThat(collection, is(nullValue()));
  }

  @Test
  public void testNotSupportedTag() {
    Metacard mockCard = getMockMetacard("resource");
    String collection = indexProvider.getCollection(mockCard);
    assertThat(collection, is(nullValue()));
  }

  @Test
  public void testNoTags() {
    Metacard mockCard = mock(Metacard.class);
    when(mockCard.getAttribute(Core.METACARD_TAGS)).thenReturn(null);
    String collection = indexProvider.getCollection(mockCard);
    assertThat(collection, is(nullValue()));
  }

  @Test
  public void testNullMetacard() {
    String collection = indexProvider.getCollection(null);
    assertThat(collection, is(nullValue()));
  }

  private Metacard getMockMetacard(String tag) {
    Metacard metacard = mock(Metacard.class);
    Attribute tagAttr = mock(Attribute.class);
    when(tagAttr.getValues()).thenReturn(Collections.singletonList(tag));
    when(metacard.getAttribute(Core.METACARD_TAGS)).thenReturn(tagAttr);
    return metacard;
  }
}
