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

import com.mchange.v2.c3p0.ComboPooledDataSource;
import ddf.catalog.data.BinaryContent;
import ddf.catalog.data.ContentType;
import ddf.catalog.data.Metacard;
import ddf.catalog.data.Result;
import ddf.catalog.data.impl.ResultImpl;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.operation.DeleteRequest;
import ddf.catalog.operation.DeleteResponse;
import ddf.catalog.operation.QueryRequest;
import ddf.catalog.operation.SourceResponse;
import ddf.catalog.operation.Update;
import ddf.catalog.operation.UpdateRequest;
import ddf.catalog.operation.UpdateResponse;
import ddf.catalog.operation.impl.CreateResponseImpl;
import ddf.catalog.operation.impl.DeleteResponseImpl;
import ddf.catalog.operation.impl.SourceResponseImpl;
import ddf.catalog.operation.impl.UpdateImpl;
import ddf.catalog.operation.impl.UpdateResponseImpl;
import ddf.catalog.source.CatalogProvider;
import ddf.catalog.source.IngestException;
import ddf.catalog.source.SourceMonitor;
import ddf.catalog.source.StorageProvider;
import ddf.catalog.source.UnsupportedQueryException;
import ddf.catalog.transform.CatalogTransformerException;
import ddf.catalog.transform.InputTransformer;
import ddf.catalog.transform.MetacardTransformer;
import ddf.catalog.util.impl.MaskableImpl;
import ddf.security.encryption.EncryptionService;
import java.beans.PropertyVetoException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class JdbcStorageProvider extends MaskableImpl implements StorageProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStorageProvider.class);

  private static final String URL_KEY = "dbUrl";

  private static final String DRIVER_KEY = "driver";

  private static final String USERNAME_KEY = "user";

  @SuppressWarnings("squid:S2068" /* Referring to the key, not a value */)
  private static final String PASSWORD_KEY = "password";

  private static final String MAX_POOL_SIZE_KEY = "maxPoolSize";

  private static final String MIN_POOL_SIZE_KEY = "minPoolSize";

  private static final String IDLE_CONN_TEST_KEY = "idleConnectionTestPeriod";

  private static final String STATEMENT_CACHE_KEY = "maxStatementCache";

  private static final String INSERT_SQL =
      "insert into METACARD_STORE values(?,?,?) ON CONFLICT (id) DO UPDATE SET update_dt=?, metacard_data=?";

  private static final String UPDATE_SQL =
      "update METACARD_STORE set update_dt=?, metacard_data=? where id=?";

  private static final String DELETE_SQL = "delete from METACARD_STORE where ID=?";

  private static final String QUERY_SQL_START =
      "select ID, METACARD_DATA from METACARD_STORE where ID IN (";

  protected static DataSource ds;

  private InputTransformer metacardDecodeTransformer;

  private MetacardTransformer metacardEncodeTransformer;

  private EncryptionService encryptionService;

  protected String dbUrl = null;

  protected String driver = null;

  protected String user = null;

  protected String password = null;

  protected int maxPoolSize = 100;

  protected int minPoolSize = 5;

  protected int idleConnectionTestPeriod = 300;

  protected int maxStatementCache = 250;

  /**
   * Constructs JdbcStorageProvider with decode and encode transformer to be used when persisting
   * Metacard data to the database. The transformer pair should be lossless when called to encode
   * and decode data.
   *
   * @param metacardDecodeTransformer - Transformer used to decode stored data
   * @param metacardEncodeTransformer - Transformer used to encode data prior to storage.
   */
  public JdbcStorageProvider(
      InputTransformer metacardDecodeTransformer,
      MetacardTransformer metacardEncodeTransformer,
      EncryptionService encryptionService) {
    Validate.notNull(metacardDecodeTransformer, "MetacardDecodeTransformer cannot be null.");
    Validate.notNull(metacardEncodeTransformer, "MetacardEncodeTransformer cannot be null.");

    this.metacardDecodeTransformer = metacardDecodeTransformer;
    this.metacardEncodeTransformer = metacardEncodeTransformer;
    this.encryptionService = encryptionService;

    init();
  }

  @Override
  public CreateResponse create(CreateRequest createRequest) throws IngestException {
    List<Metacard> metacards = createRequest.getMetacards();
    insertMetacards(metacards);
    return new CreateResponseImpl(createRequest, createRequest.getProperties(), metacards);
  }

  @Override
  public UpdateResponse update(UpdateRequest updateRequest) throws IngestException {
    if (!StringUtils.equals(updateRequest.getAttributeName(), Metacard.ID)) {
      throw new IngestException("Unable to update by anything other than Metacard.ID");
    }
    List<Map.Entry<Serializable, Metacard>> requestedUpdates = updateRequest.getUpdates();

    try {
      Set<String> identifiers =
          requestedUpdates
              .stream()
              .map(Map.Entry::getKey)
              .map(Serializable::toString)
              .filter(Objects::nonNull)
              .collect(Collectors.toSet());

      Map<String, Metacard> oldMetacardMap =
          getMetacards(identifiers)
              .stream()
              .filter(Objects::nonNull)
              .collect(Collectors.toMap(Metacard::getId, Function.identity()));

      List<Metacard> updatedCards = new ArrayList<>();
      for (Entry<Serializable, Metacard> requestedUpdate : requestedUpdates) {
        Metacard value = requestedUpdate.getValue();
        value.setSourceId(getId());
        updatedCards.add(value);
      }

      updateMetacards(updatedCards);

      List<Update> updates =
          updatedCards
              .stream()
              .map(mc -> new UpdateImpl(mc, oldMetacardMap.get(mc.getId())))
              .collect(Collectors.toList());
      return new UpdateResponseImpl(updateRequest, updateRequest.getProperties(), updates);
    } catch (UnsupportedQueryException e) {
      throw new IngestException(e);
    }
  }

  @Override
  public DeleteResponse delete(DeleteRequest deleteRequest) throws IngestException {
    String deleteAttribute = deleteRequest.getAttributeName();
    if (!StringUtils.equals(deleteAttribute, Metacard.ID)) {
      throw new IngestException(
          "Delete from SolrStorageProvider by anything other than Metacard.ID not supported");
    }

    Set<String> ids =
        deleteRequest
            .getAttributeValues()
            .stream()
            .map(Object::toString)
            .collect(Collectors.toSet());

    List<Metacard> deletedMetacards;
    try {
      deletedMetacards = getMetacards(ids);
    } catch (UnsupportedQueryException e) {
      throw new IngestException("Unable to delete metacards", e);
    }

    try (Connection conn = ds.getConnection()) {
      for (String id : ids) {
        try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
          ps.setString(1, id);
          int numRecords = ps.executeUpdate();
          LOGGER.trace("Deleted {} records", numRecords);
        }
      }
    } catch (SQLException e) {
      throw new IngestException("Unable to get DB connection: " + dbUrl, e);
    }

    return new DeleteResponseImpl(deleteRequest, deleteRequest.getProperties(), deletedMetacards);
  }

  @Override
  public SourceResponse query(QueryRequest queryRequest) throws UnsupportedQueryException {
    throw new UnsupportedQueryException("Generic queries to SolrStorageProvider not supported.");
  }

  /**
   * for a solr storage provider quering by IDs might not be optimal. hence handling as a normal
   * query
   */
  public SourceResponse queryByIds(
      QueryRequest queryRequest, Map<String, Serializable> properties, List<String> ids)
      throws UnsupportedQueryException {
    List<Metacard> metacards = getMetacards(new HashSet<>(ids));
    List<Result> results = metacards.stream().map(ResultImpl::new).collect(Collectors.toList());
    return new SourceResponseImpl(queryRequest, properties, results);
  }

  @Override
  public void shutdown() {
    destroy();
  }

  @Override
  public boolean isAvailable() {
    if (ds != null) {
      try (Connection conn = ds.getConnection()) {
        if (LOGGER.isDebugEnabled()) {
          if (ds instanceof ComboPooledDataSource) {
            ComboPooledDataSource poolDs = (ComboPooledDataSource) ds;
            LOGGER.debug("Number of Connections: {}", poolDs.getNumConnections());
            LOGGER.debug("Number of Connections: {}", poolDs.getNumConnectionsAllUsers());
            LOGGER.debug("Number of Busy Connections: {}", poolDs.getNumBusyConnections());
            LOGGER.debug("Number of Busy Connections: {}", poolDs.getNumBusyConnectionsAllUsers());
            LOGGER.debug("Number of Idle Connections: {}", poolDs.getNumIdleConnections());
            LOGGER.debug("Number of Idle Connections: {}", poolDs.getNumIdleConnectionsAllUsers());
            LOGGER.debug(
                "Number of Orphaned Connections: {}", poolDs.getNumUnclosedOrphanedConnections());
            LOGGER.debug(
                "Number of Orphaned Connections: {}",
                poolDs.getNumUnclosedOrphanedConnectionsAllUsers());
          }
        }
        return conn != null && !conn.isClosed();
      } catch (SQLException e) {
        LOGGER.trace("Unable to test data source connection", e);
      }
    }
    return false;
  }

  @Override
  public boolean isAvailable(SourceMonitor callback) {
    boolean avail = isAvailable();
    if (avail) {
      callback.setAvailable();
    } else {
      callback.setUnavailable();
    }
    return avail;
  }

  @Override
  public Set<ContentType> getContentTypes() {
    return Collections.emptySet();
  }

  public void refresh(Map<String, Object> configuration) {
    if (configuration == null) {
      LOGGER.debug("Null configuration");
      return;
    }

    setDbUrl((String) configuration.get(URL_KEY));
    setDriver((String) configuration.get(DRIVER_KEY));
    setUser((String) configuration.get(USERNAME_KEY));
    setPassword((String) configuration.get(PASSWORD_KEY));

    Integer configPoolSize = (Integer) configuration.get(MAX_POOL_SIZE_KEY);
    if (configPoolSize != null) {
      setMaxPoolSize(configPoolSize);
    }

    Integer configMinPoolSize = (Integer) configuration.get(MIN_POOL_SIZE_KEY);
    if (configMinPoolSize != null) {
      setMinPoolSize(configMinPoolSize);
    }

    Integer configIdleConnTimeout = (Integer) configuration.get(IDLE_CONN_TEST_KEY);
    if (configIdleConnTimeout != null) {
      setIdleConnectionTestPeriod(configIdleConnTimeout);
    }

    Integer configStatementCache = (Integer) configuration.get(STATEMENT_CACHE_KEY);
    if (configStatementCache != null) {
      setMaxStatementCache(configStatementCache);
    }

    init();
  }

  public void setDbUrl(String dbUrl) {
    this.dbUrl = dbUrl;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setPassword(String password) {
    if (encryptionService != null) {
      this.password = encryptionService.decryptValue(password);
    } else {
      this.password = password;
    }
  }

  public void setMaxPoolSize(int maxPoolSize) {
    this.maxPoolSize = maxPoolSize;
  }

  public void setMinPoolSize(int minPoolSize) {
    this.minPoolSize = minPoolSize;
  }

  public void setIdleConnectionTestPeriod(int idleConnectionTestPeriod) {
    this.idleConnectionTestPeriod = idleConnectionTestPeriod;
  }

  public void setMaxStatementCache(int maxStatementCache) {
    this.maxStatementCache = maxStatementCache;
  }

  public void setDataSource(DataSource dataSource) {
    this.ds = dataSource;
  }

  public void init() {
    closeDataSource();

    if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(dbUrl)) {
      try {
        createDB();
        initDataSource();
      } catch (Exception e) {
        LOGGER.warn(
            "Unable to initialize storage to: {}. Please view DEBUG log for more information.",
            dbUrl);
        LOGGER.debug("JDBC storage {} not initialized", dbUrl, e);
      }
    }
  }

  public void destroy() {
    closeDataSource();
  }

  private void closeDataSource() {
    if (ds instanceof ComboPooledDataSource) {
      ((ComboPooledDataSource) ds).close();
    }
  }

  private void createDB() {
    LOGGER.info("Creating Storage DB at URL: {}", dbUrl);
    Flyway flyway = Flyway.configure().dataSource(dbUrl, user, password).load();
    flyway.migrate();
  }

  private void initDataSource() {
    try {
      ComboPooledDataSource ds = new ComboPooledDataSource();
      ds.setDriverClass(driver); // loads the jdbc driver
      ds.setJdbcUrl(dbUrl);
      ds.setUser(user);
      ds.setPassword(password);
      ds.setMinPoolSize(minPoolSize);
      ds.setMaxPoolSize(maxPoolSize);
      ds.setAcquireIncrement(5);
      ds.setTestConnectionOnCheckout(false);
      ds.setTestConnectionOnCheckin(false);
      ds.setIdleConnectionTestPeriod(idleConnectionTestPeriod);
      ds.setMaxStatements(maxStatementCache);
      ds.setNumHelperThreads(Runtime.getRuntime().availableProcessors() * 2);
      ds.setMaxConnectionAge((int) TimeUnit.HOURS.toSeconds(4));
      ds.setMaxIdleTime((int) TimeUnit.MINUTES.toSeconds(30));
      ds.setMaxIdleTimeExcessConnections((int) TimeUnit.MINUTES.toSeconds(30));
      setDataSource(ds);
    } catch (PropertyVetoException e) {
      LOGGER.error("Failed to create a connection pool");
    }
  }

  private void insertMetacards(List<Metacard> metacards) throws IngestException {
    long insertTime = System.currentTimeMillis();

    List<Metacard> cardsToUpdate = new ArrayList<>();
    Connection conn = null;
    try {
      conn = ds.getConnection();
      conn.setAutoCommit(false);
      try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
        for (Metacard metacard : metacards) {
          String encodedMetacard = getEncodedMetacard(metacard);
          ps.setString(1, metacard.getId());
          ps.setLong(2, insertTime);
          ps.setString(3, encodedMetacard);
          ps.setLong(4, insertTime);
          ps.setString(5, encodedMetacard);
          ps.executeUpdate();
        }
      }
      conn.commit();
    } catch (SQLException e) {
      LOGGER.debug("SQL Exception encountered while storing data", e);
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException ex) {
          LOGGER.debug("Unable to rollback transaction", ex);
        }
      }
      throw new IngestException("Unable to insert metadata to: " + dbUrl, e);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          LOGGER.debug("Unable to close JDBC Connection", e);
        }
      }
    }

    updateMetacards(cardsToUpdate);

    if (LOGGER.isTraceEnabled()) {
      long totalTime = System.currentTimeMillis() - insertTime;
      LOGGER.trace("Total time to insert records: {} ms", totalTime);
    }
  }

  private void updateMetacards(List<Metacard> metacards) throws IngestException {
    long insertTime = System.currentTimeMillis();

    if (CollectionUtils.isEmpty(metacards)) {
      return;
    }

    Connection conn = null;
    try {
      conn = ds.getConnection();
      conn.setAutoCommit(false);
      try (PreparedStatement ps = conn.prepareStatement(UPDATE_SQL)) {
        for (Metacard metacard : metacards) {
          String encodedMetacard = getEncodedMetacard(metacard);
          ps.setLong(1, insertTime);
          ps.setString(2, encodedMetacard);
          ps.setString(3, metacard.getId());
          ps.addBatch();
        }
        ps.executeBatch();
      }
      conn.commit();
    } catch (SQLException e) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (SQLException ex) {
          LOGGER.debug("Unable to rollback transaction", ex);
        }
      }
      throw new IngestException("Unable to get DB connection: " + dbUrl, e);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          LOGGER.debug("Unable to close JDBC connection", e);
        }
      }
    }
  }

  protected String getEncodedMetacard(Metacard metacard) throws IngestException {
    boolean isSourceIdSet = (metacard.getSourceId() != null && !"".equals(metacard.getSourceId()));

    if (StringUtils.isBlank(metacard.getId())) {
      throw new IngestException(
          "Metacards should have an ID by the time they are stored in the storage provider.");
    }

    if (!isSourceIdSet) {
      metacard.setSourceId(getId());
    }

    String encodedMetacard = null;
    try {
      encodedMetacard = encodeMetacard(metacard);
      return encodedMetacard;
    } catch (CatalogTransformerException e) {
      throw new IngestException("Unable to encode metacard to XML: " + metacard, e);
    }
  }

  protected String encodeMetacard(Metacard metacard) throws CatalogTransformerException {
    BinaryContent binaryContent = metacardEncodeTransformer.transform(metacard, null);
    String encodedMetacard = null;
    if (binaryContent.getMimeType().getPrimaryType().equals("text")) {
      try {
        encodedMetacard = new String(binaryContent.getByteArray());
      } catch (IOException e) {
        LOGGER.debug("Unable to get metacard data", e);
      }
    }
    return encodedMetacard;
  }

  private Metacard getMetacard(String id, String data) {
    if (data != null) {
      try {
        return metacardDecodeTransformer.transform(new ByteArrayInputStream(data.getBytes()), id);
      } catch (IOException | CatalogTransformerException e) {
        LOGGER.debug("Unable to transform {} to metacard", data, e);
      }
    }
    return null;
  }

  private List<Metacard> getMetacards(Set<String> ids) throws UnsupportedQueryException {
    if (CollectionUtils.isEmpty(ids)) {
      return Collections.emptyList();
    }

    List<Metacard> metacards = new ArrayList<>(ids.size());
    ResultSet rs = null;
    long startTime = System.currentTimeMillis();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(getQuery(ids.size()))) {
      int index = 1;
      for (String id : ids) {
        LOGGER.trace("Setting query index[{}]: {}", index, id);
        ps.setString(index, id);
        index++;
      }
      rs = ps.executeQuery();

      long psExecuteElapsedTime = System.currentTimeMillis() - startTime;
      while (rs.next()) {
        String id = rs.getString("ID");
        String metacardXml = rs.getString("METACARD_DATA");
        Metacard metacard = getMetacard(id, metacardXml);
        metacards.add(metacard);
      }

      if (LOGGER.isTraceEnabled()) {
        long totalElapsedTime = System.currentTimeMillis() - startTime;
        long xmlDecodeElapsedTime = totalElapsedTime - psExecuteElapsedTime;

        LOGGER.trace(
            "PS Execute elapsed time {} ms and XML Decode elapsed time {} ms",
            psExecuteElapsedTime,
            xmlDecodeElapsedTime);
      }

    } catch (SQLException e) {
      throw new UnsupportedQueryException("Unable to get DB connection: " + dbUrl, e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          LOGGER.trace("Unable to close result set", e);
        }
      }
    }
    return metacards;
  }

  private String getQuery(int numIds) {
    StringBuilder query = new StringBuilder(QUERY_SQL_START);
    for (int i = 0; i < numIds; i++) {
      if (i > 0) {
        query.append(",");
      }
      query.append("?");
    }
    query.append(")");

    return query.toString();
  }
}
