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
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ddf.catalog.data.Metacard;
import ddf.catalog.data.impl.MetacardImpl;
import ddf.catalog.data.types.Core;
import ddf.catalog.filter.FilterBuilder;
import ddf.catalog.filter.proxy.builder.GeotoolsFilterBuilder;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.DeleteResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateRequestImpl;
import ddf.catalog.operation.impl.DeleteRequestImpl;
import ddf.catalog.operation.impl.QueryImpl;
import ddf.catalog.operation.impl.QueryRequestImpl;
import ddf.catalog.operation.impl.UpdateRequestImpl;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.SourceMonitor;
import ddf.catalog.source.StorageProvider;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.transformer.api.MetacardMarshaller;
import ddf.catalog.transformer.xml.MetacardMarshallerImpl;
import ddf.catalog.transformer.xml.PrintWriterProviderImpl;
import ddf.catalog.transformer.xml.XmlInputTransformer;
import ddf.catalog.transformer.xml.XmlMetacardTransformer;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.sql.DataSource;
import org.codice.ddf.parser.Parser;
import org.codice.ddf.parser.xml.XmlParser;
import org.junit.Before;
import org.junit.Test;
import org.opengis.filter.Filter;

public class JdbcStorageProviderTest {

  private JdbcStorageProvider storageProvider;

  private XmlInputTransformer encoderTransformer;

  private XmlMetacardTransformer decoderTransformer;

  protected FilterBuilder filterBuilder = new GeotoolsFilterBuilder();

  protected DataSource mockDs;

  protected Connection mockConn;

  public JdbcStorageProviderTest() {
    storageProvider = getStorageProvider();
  }

  @Before
  public void setUp() throws Exception {
    mockDs = mock(DataSource.class);
    mockConn = mock(Connection.class);
    when(mockDs.getConnection()).thenReturn(mockConn);
    storageProvider.setDataSource(mockDs);
  }

  @Test
  public void testCreate() throws Exception {
    PreparedStatement mockPs = mock(PreparedStatement.class);
    when(mockPs.executeUpdate()).thenReturn(2);
    when(mockConn.prepareStatement(any())).thenReturn(mockPs);

    CreateResponse response = createRecord(storageProvider);
    assertThat(response.getCreatedMetacards().size(), equalTo(2));
  }

  @Test(expected = UnsupportedQueryException.class)
  public void testQuery() throws Exception {
    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    QueryRequest request = new QueryRequestImpl(new QueryImpl(filter));
    storageProvider.query(request);
  }

  @Test
  public void testQueryById() throws Exception {
    PreparedStatement mockPs = mock(PreparedStatement.class);
    ResultSet mockResults = getMockResults();

    when(mockPs.executeQuery()).thenReturn(mockResults);
    when(mockConn.prepareStatement(any())).thenReturn(mockPs);

    Filter filter = filterBuilder.attribute(Metacard.ANY_TEXT).is().like().text("*");
    SourceResponse response =
        storageProvider.queryByIds(
            new QueryRequestImpl(new QueryImpl(filter)),
            Collections.emptyMap(),
            Collections.singletonList("testId"));
    assertThat(response.getHits(), is(1L));
    Metacard mc = response.getResults().iterator().next().getMetacard();
    assertThat(mc.getAttribute(Core.TITLE).getValue().toString(), is("Metacard-1"));
  }

  @Test
  public void testUpdate() throws Exception {
    PreparedStatement mockPs = mock(PreparedStatement.class);
    ResultSet mockResults = getMockResults();

    when(mockPs.executeUpdate()).thenReturn(1);
    when(mockPs.executeQuery()).thenReturn(mockResults);
    when(mockConn.prepareStatement(any())).thenReturn(mockPs);

    MetacardImpl mc = new MetacardImpl();
    mc.setId("1");
    mc.setTitle("Metacard-1");

    UpdateRequest request =
        new UpdateRequestImpl(new String[] {mc.getId()}, Collections.singletonList(mc));
    UpdateResponse response = storageProvider.update(request);
    assertThat(response.getUpdatedMetacards().size(), equalTo(1));
    assertThat(
        response
            .getUpdatedMetacards()
            .iterator()
            .next()
            .getNewMetacard()
            .getAttribute(Core.TITLE)
            .getValue()
            .toString(),
        is("Metacard-1"));
  }

  @Test(expected = IngestException.class)
  public void testUpdateByNonId() throws Exception {
    PreparedStatement mockPs = mock(PreparedStatement.class);
    ResultSet mockResults = getMockResults();

    when(mockPs.executeUpdate()).thenReturn(1);
    when(mockPs.executeQuery()).thenReturn(mockResults);
    when(mockConn.prepareStatement(any())).thenReturn(mockPs);

    MetacardImpl mc = new MetacardImpl();
    mc.setId("1");
    mc.setTitle("Metacard-1");

    List<Entry<Serializable, Metacard>> updateMap =
        Collections.singletonList(new SimpleEntry<>("Metacard-1", mc));
    UpdateRequest request = new UpdateRequestImpl(updateMap, Core.TITLE, Collections.emptyMap());
    storageProvider.update(request);
  }

  @Test
  public void testDelete() throws Exception {
    PreparedStatement mockPs = mock(PreparedStatement.class);
    ResultSet mockResults = getMockResults();

    when(mockPs.executeUpdate()).thenReturn(1);
    when(mockPs.executeQuery()).thenReturn(mockResults);
    when(mockConn.prepareStatement(any())).thenReturn(mockPs);

    DeleteRequest request = new DeleteRequestImpl("1");
    DeleteResponse response = storageProvider.delete(request);
    assertThat(response.getDeletedMetacards().size(), equalTo(1));
    assertThat(
        response
            .getDeletedMetacards()
            .iterator()
            .next()
            .getAttribute(Core.TITLE)
            .getValue()
            .toString(),
        is("Metacard-1"));
  }

  private CreateResponse createRecord(StorageProvider storageProvider) throws Exception {
    List<Metacard> list =
        Arrays.asList(
            new MockMetacard(Library.getFlagstaffRecord()),
            new MockMetacard(Library.getTampaRecord()));

    CreateRequest request = new CreateRequestImpl(list);
    return storageProvider.create(request);
  }

  @Test
  public void testShutdown() {
    storageProvider.shutdown();
  }

  @Test
  public void testIsAvailable() {
    SourceMonitor availCb = mock(SourceMonitor.class);
    boolean available = storageProvider.isAvailable();
    assertThat(available, is(true));
    available = storageProvider.isAvailable(availCb);
    assertThat(available, is(true));
    verify(availCb, times(1)).setAvailable();
  }

  @Test
  public void testUnavailable() {
    JdbcStorageProvider unavailProvider = getStorageProvider();
    unavailProvider.setDataSource(null);
    SourceMonitor availCb = mock(SourceMonitor.class);

    boolean available = unavailProvider.isAvailable();
    assertThat(available, is(false));
    available = unavailProvider.isAvailable(availCb);
    assertThat(available, is(false));
    verify(availCb, times(1)).setUnavailable();
  }

  @Test
  public void testMaskId() {
    String maskedId = "masked-id";
    storageProvider.maskId(maskedId);
    assertThat(storageProvider.getId(), is(maskedId));
  }

  @Test
  public void testRefreshNoData() {
    storageProvider.refresh(null);
    assertThat(storageProvider.maxPoolSize, is(100));
  }

  @Test
  public void testRefresh() {
    Map<String, Object> config = new HashMap<>();
    String url = "jdbc://test/url";
    String driver = "mock.driver";
    String user = "testUser";
    String password = "testPassword";

    config.put("dbUrl", url);
    config.put("driver", driver);
    config.put("user", user);
    config.put("password", password);
    config.put("maxPoolSize", 200);
    config.put("minPoolSize", 10);
    config.put("idleConnectionTestPeriod", 500);
    config.put("maxStatementCache", 450);

    storageProvider.refresh(config);
    assertThat(storageProvider.dbUrl, is(url));
    assertThat(storageProvider.driver, is(driver));
    assertThat(storageProvider.user, is(user));
    assertThat(storageProvider.password, is(password));
    assertThat(storageProvider.maxPoolSize, is(200));
    assertThat(storageProvider.minPoolSize, is(10));
    assertThat(storageProvider.idleConnectionTestPeriod, is(500));
    assertThat(storageProvider.maxStatementCache, is(450));
  }

  private ResultSet getMockResults() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true).thenReturn(false);
    when(rs.getString("ID")).thenReturn("testId");
    when(rs.getString("METACARD_DATA")).thenReturn(Library.getTestMetacard());
    return rs;
  }

  private JdbcStorageProvider getStorageProvider() {
    Parser parser = new XmlParser();
    encoderTransformer = new XmlInputTransformer(parser);
    encoderTransformer.setMetacardTypes(Collections.singletonList(MetacardImpl.BASIC_METACARD));
    MetacardMarshaller metacardMarshaller =
        new MetacardMarshallerImpl(parser, new PrintWriterProviderImpl());
    decoderTransformer = new XmlMetacardTransformer(metacardMarshaller);
    return new JdbcStorageProvider(encoderTransformer, decoderTransformer, null);
  }
}
