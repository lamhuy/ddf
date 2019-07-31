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
package ddf.catalog.provider.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.impl.MetacardImpl;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.DeleteRequestImpl;
import ddf.catalog.source.StorageProvider;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.transformer.api.MetacardMarshaller;
import ddf.catalog.transformer.xml.MetacardMarshallerImpl;
import ddf.catalog.transformer.xml.PrintWriterProviderImpl;
import ddf.catalog.transformer.xml.XmlInputTransformer;
import ddf.catalog.transformer.xml.XmlMetacardTransformer;
import java.util.Arrays;
import java.util.List;
import org.codice.ddf.parser.Parser;
import org.codice.ddf.parser.xml.XmlParser;
import org.junit.Before;
import org.junit.Test;

public class JdbcStorageProviderTest {

  private JdbcStorageProvider storageProvider;
  private XmlInputTransformer encoderTransformer;
  private XmlMetacardTransformer decoderTransformer;

  public JdbcStorageProviderTest() {
    Parser parser = new XmlParser();
    encoderTransformer = new XmlInputTransformer(parser);
    encoderTransformer.setMetacardTypes(Arrays.asList(MetacardImpl.BASIC_METACARD));
    MetacardMarshaller metacardMarshaller =
        new MetacardMarshallerImpl(parser, new PrintWriterProviderImpl());
    decoderTransformer = new XmlMetacardTransformer(metacardMarshaller);
    storageProvider = new JdbcStorageProvider(encoderTransformer, decoderTransformer);
  }

  @Before
  public void setup() {}

  @Test
  public void testCreate() throws Exception {
    CreateResponse response = createRecord(storageProvider);
    assertThat(response.getCreatedMetacards().size(), equalTo(2));
  }

  @Test(expected = UnsupportedQueryException.class)
  public void testQuery() throws Exception {
    CreateResponse records = createRecord(storageProvider);
    //    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    //    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));
    //
    //    when(provider.query(request))
    //        .thenReturn(
    //            new SourceResponseImpl(
    //                request,
    //                records
    //                    .getCreatedMetacards()
    //                    .stream()
    //                    .map(c -> new ResultImpl(c))
    //                    .collect(Collectors.toList()),
    //                2L));
    //
    //    storageProvider.query(request);
  }

  @Test
  public void testQueryById() throws Exception {
    CreateResponse records = createRecord(storageProvider);
    //    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    //    SolrMetacardClient mockClient = mock(SolrMetacardClient.class);
    //    List<SolrDocument> docs = getSolrDocs(records.getCreatedMetacards());
    //    when(mockClient.getSolrDocs(any())).thenReturn(docs);
    //    when(provider.getSolrMetacardClient()).thenReturn(mockClient);
    //
    //    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));
    //
    //    SourceResponse response =
    //        storageProvider.queryByIds(
    //            request,
    //            records
    //                .getCreatedMetacards()
    //                .stream()
    //                .map(Metacard::getId)
    //                .collect(Collectors.toList()));
    //    assertThat(response.getHits(), equalTo(2L));
  }

  @Test
  public void testUpdate() throws Exception {
    CreateResponse records = createRecord(storageProvider);
    //    SolrMetacardClient mockClient = mock(SolrMetacardClient.class);
    //    List<SolrDocument> docs = getSolrDocs(records.getCreatedMetacards());
    //    when(mockClient.getSolrDocs(any())).thenReturn(docs);
    //    when(provider.getSolrMetacardClient()).thenReturn(mockClient);
    //
    //    UpdateRequest request =
    //        new UpdateRequestImpl(
    //
    // records.getCreatedMetacards().stream().map(Metacard::getId).toArray(String[]::new),
    //            records.getCreatedMetacards());
    //    UpdateResponse response = storageProvider.update(request);
    //    assertThat(response.getUpdatedMetacards().size(), equalTo(2));
  }

  @Test
  public void testDelete() throws Exception {
    CreateResponse records = createRecord(storageProvider);
    DeleteRequest request =
        new DeleteRequestImpl(
            records.getCreatedMetacards().stream().map(c -> c.getId()).toArray(String[]::new));
    storageProvider.delete(request);
  }

  private CreateResponse createRecord(StorageProvider storageProvider) throws Exception {
    List<Metacard> list =
        Arrays.asList(
            new MockMetacard(Library.getFlagstaffRecord()),
            new MockMetacard(Library.getTampaRecord()));

    CreateRequest request = new CreateRequestImpl(list);
    //    when(this.storageProvider.provider.create(request))
    //        .thenReturn(new CreateResponseImpl(request, request.getProperties(), list));

    return storageProvider.create(request);
  }

  @Test
  public void testShutdown() {
    storageProvider.shutdown();
  }

  @Test
  public void testIsAvailable() {
    storageProvider.isAvailable();
    //    when(provider.isAvailable()).thenReturn(true);
    //    when(provider.isAvailable(any())).thenReturn(true);
    storageProvider.isAvailable(null);
  }

  @Test
  public void testGetContentTypes() {
    storageProvider.getContentTypes();
  }

  @Test
  public void testMaskId() {
    storageProvider.maskId("id");
  }

  //  private List<SolrDocument> getSolrDocs(List<Metacard> metacards) throws Exception {
  //    List<SolrDocument> solrDocs = new ArrayList<>();
  //    for (Metacard metacard : metacards) {
  //      SolrDocument doc = new SolrDocument();
  //      doc.addField(JdbcStorageProvider.ID_FIELD, metacard.getId());
  //      doc.addField(JdbcStorageProvider.DATA_FIELD, storageProvider.encodeMetacard(metacard));
  //      solrDocs.add(doc);
  //    }
  //    return solrDocs;
  //  }
}
