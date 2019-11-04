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
package org.codice.ddf.spatial.ogc.wfs.featuretransformer.impl;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ddf.catalog.data.AttributeDescriptor;
import ddf.catalog.data.Metacard;
import ddf.catalog.data.impl.MetacardImpl;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.builder.NoErrorHandlerBuilder;
import org.apache.camel.component.bean.ProxyHelper;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.codice.ddf.spatial.ogc.wfs.catalog.WfsFeatureCollection;
import org.codice.ddf.spatial.ogc.wfs.catalog.common.FeatureMetacardType;
import org.codice.ddf.spatial.ogc.wfs.featuretransformer.FeatureTransformationService;
import org.codice.ddf.spatial.ogc.wfs.featuretransformer.FeatureTransformer;
import org.codice.ddf.spatial.ogc.wfs.featuretransformer.WfsMetadata;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class FeatureTransformationServiceTest {

  private static final int FEATURE_MEMBER_COUNT = 10;

  private CamelContext camelContext;

  private FeatureTransformationService featureTransformationService;

  private List<FeatureTransformer> transformerList;

  @Before
  public void setup() throws Exception {
    setupTransformers();
    SimpleRegistry registry = new SimpleRegistry();
    registry.put("wfsTransformerProcessor", new WfsTransformerProcessor(transformerList));

    this.camelContext = new DefaultCamelContext(registry);
    camelContext.setTracing(true);
    camelContext.addRoutes(new WfsRouteBuilder());
    camelContext.setErrorHandlerBuilder(new NoErrorHandlerBuilder());

    Endpoint endpoint = camelContext.getEndpoint(WfsRouteBuilder.FEATURECOLLECTION_ENDPOINT_URL);
    featureTransformationService =
        ProxyHelper.createProxy(endpoint, FeatureTransformationService.class);
    camelContext.start();
  }

  @After
  public void cleanup() throws Exception {
    this.camelContext.stop();
  }

  @Test
  public void testApplyWithFeatureMembers() {
    validateTenMetacards("/Neverland.xml", "featureMember");
  }

  @Test
  public void testApplyNoFeatureMembers() {
    validateTenMetacards("/Neverland2.xml", "PeterPan");
  }

  private void validateTenMetacards(String inputFileName, String featureNodeName) {
    InputStream inputStream =
        new BufferedInputStream(
            FeatureTransformationServiceTest.class.getResourceAsStream(inputFileName));

    WfsMetadata wfsMetadata = mock(WfsMetadata.class);
    when(wfsMetadata.getFeatureMemberNodeNames())
        .thenReturn(Collections.singletonList(featureNodeName));

    WfsFeatureCollection wfsFeatureCollection =
        featureTransformationService.apply(inputStream, wfsMetadata);
    ArgumentCaptor<InputStream> inputStreamArgumentCaptor =
        ArgumentCaptor.forClass(InputStream.class);
    ArgumentCaptor<WfsMetadata> wfsMetadataArgumentCaptor =
        ArgumentCaptor.forClass(WfsMetadata.class);
    verify(transformerList.get(0), times(FEATURE_MEMBER_COUNT))
        .apply(inputStreamArgumentCaptor.capture(), wfsMetadataArgumentCaptor.capture());

    for (int i = 0; i < FEATURE_MEMBER_COUNT; i++) {
      assertThat(inputStreamArgumentCaptor.getAllValues().get(i), notNullValue());
      assertThat(wfsMetadataArgumentCaptor.getAllValues().get(i), notNullValue());
    }

    assertThat(wfsFeatureCollection.getNumberOfFeatures(), is(10L));
    assertThat(wfsFeatureCollection.getFeatureMembers(), hasSize(10));
    assertThat(wfsFeatureCollection.getFeatureMembers(), everyItem(hasNoExternalWfsAttributes()));
  }

  @Test
  public void testApplyBadXML() {
    InputStream inputStream =
        new BufferedInputStream(
            FeatureTransformationServiceTest.class.getResourceAsStream("/Broken.xml"));

    WfsMetadata wfsMetadata = mock(WfsMetadata.class);
    when(wfsMetadata.getFeatureMemberNodeNames())
        .thenReturn(Collections.singletonList("featureMember"));

    WfsFeatureCollection wfsFeatureCollection =
        featureTransformationService.apply(inputStream, wfsMetadata);
    ArgumentCaptor<InputStream> inputStreamArgumentCaptor =
        ArgumentCaptor.forClass(InputStream.class);
    ArgumentCaptor<WfsMetadata> wfsMetadataArgumentCaptor =
        ArgumentCaptor.forClass(WfsMetadata.class);
    verify(transformerList.get(0), times(0))
        .apply(inputStreamArgumentCaptor.capture(), wfsMetadataArgumentCaptor.capture());

    assertThat(wfsFeatureCollection.getNumberOfFeatures(), is(10L));
    assertThat(wfsFeatureCollection.getFeatureMembers(), hasSize(0));
  }

  @Test
  public void testFeatureMembersWithNoNumberOfFeaturesAttribute() throws Exception {
    try (final InputStream inputStream =
        getClass().getResourceAsStream("/NeverlandNoNumberOfFeaturesAttribute.xml")) {

      final WfsMetadata wfsMetadata = mock(WfsMetadata.class);
      when(wfsMetadata.getFeatureMemberNodeNames())
          .thenReturn(Collections.singletonList("featureMember"));

      final WfsFeatureCollection wfsFeatureCollection =
          featureTransformationService.apply(inputStream, wfsMetadata);

      assertThat(wfsFeatureCollection.getNumberOfFeatures(), is(10L));
      assertThat(wfsFeatureCollection.getFeatureMembers(), hasSize(10));
    }
  }

  private void setupTransformers() throws Exception {
    transformerList = new ArrayList<>();
    FeatureTransformer mockTransformer = mock(FeatureTransformer.class);
    Optional optional = Optional.of(new MetacardImpl(getFeatureMetacardType()));
    when(mockTransformer.apply(any(InputStream.class), any(WfsMetadata.class)))
        .thenReturn(optional);
    transformerList.add(mockTransformer);
  }

  private FeatureMetacardType getFeatureMetacardType() throws Exception {
    final XmlSchema schema = loadSchema("Neverland.xsd");
    return new FeatureMetacardType(
        schema,
        new QName("http://www.neverland.org/peter/pan", "PeterPan", "neverland"),
        emptyList(),
        "http://opengis.net/gml");
  }

  private XmlSchema loadSchema(final String schemaFile) throws Exception {
    final XmlSchemaCollection schemaCollection = new XmlSchemaCollection();
    try (final InputStream schemaStream = new FileInputStream("src/test/resources/" + schemaFile)) {
      return schemaCollection.read(new StreamSource(schemaStream));
    }
  }

  private static Matcher<Metacard> hasNoExternalWfsAttributes() {
    return new HasNoExternalWfsAttributesMatcher("a metacard with no 'ext.PeterPan.*' attributes");
  }

  private static class HasNoExternalWfsAttributesMatcher extends CustomTypeSafeMatcher<Metacard> {

    private HasNoExternalWfsAttributesMatcher(final String description) {
      super(description);
    }

    @Override
    protected boolean matchesSafely(final Metacard item) {
      return item.getMetacardType()
          .getAttributeDescriptors()
          .stream()
          .map(AttributeDescriptor::getName)
          .noneMatch(name -> name.startsWith("ext.PeterPan."));
    }
  }
}
