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
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CatalogProvider} implementation using Apache Solr */
public class JdbcStorageProvider extends MaskableImpl implements StorageProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStorageProvider.class);

  private static final String URL_KEY = "dbUrl";

  private static final String USERNAME_KEY = "user";

  @SuppressWarnings("squid:S2068" /* Referring to the key, not a value */)
  private static final String PASSWORD_KEY = "password";

  private static final String POOLSIZE_KEY = "poolSize";

  private static final String INSERT_SQL = "insert into METACARD_STORE values(?,?,?)";

  private static final String UPDATE_SQL =
      "update METACARD_STORE set update_dt=?, metacard_data=? where id=?";

  private static final String DELETE_SQL = "delete from METACARD_STORE where ID=?";

  private static final String QUERY_SQL_START =
      "select ID, METACARD_DATA from METACARD_STORE where ID IN (";

  private static BasicDataSource ds = new BasicDataSource();

  protected InputTransformer metacardDecodeTransformer;

  protected MetacardTransformer metacardEncodeTransformer;

  private String dbUrl = null;

  private String user = null;

  private String password = null;

  private int poolSize = 100;

  /**
   * @param metacardDecodeTransformer
   * @param metacardEncodeTransformer
   */
  public JdbcStorageProvider(
      InputTransformer metacardDecodeTransformer, MetacardTransformer metacardEncodeTransformer) {
    Validate.notNull(metacardDecodeTransformer, "MetacardDecodeTransformer cannot be null.");
    Validate.notNull(metacardEncodeTransformer, "MetacardEncodeTransformer cannot be null.");

    this.metacardDecodeTransformer = metacardDecodeTransformer;
    this.metacardEncodeTransformer = metacardEncodeTransformer;
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

      List<Metacard> updatedCards =
          requestedUpdates
              .stream()
              .map(Map.Entry::getValue)
              .peek(mc -> mc.setSourceId(getId()))
              .collect(Collectors.toList());
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
    try {
      List<Metacard> deletedMetacards = getMetacards(ids);
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
    } catch (UnsupportedQueryException e) {
      throw new IngestException("Unable to delete metacards", e);
    }
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

  public void shutdown() {}

  @Override
  public boolean isAvailable() {
    return ds != null && !ds.isClosed();
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

  @Override
  public void maskId(String id) {
    super.maskId(id);
  }

  public void refresh(Map<String, Object> configuration) {
    if (configuration == null) {
      LOGGER.debug("Null configuration");
      return;
    }

    setDbUrl((String) configuration.get(URL_KEY));
    setUser((String) configuration.get(USERNAME_KEY));
    setPassword((String) configuration.get(PASSWORD_KEY));

    Integer poolSize = (Integer) configuration.get(POOLSIZE_KEY);
    if (poolSize != null) {
      setPoolSize(poolSize);
    }

    init();
  }

  public void setDbUrl(String dbUrl) {
    this.dbUrl = dbUrl;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setPoolSize(int poolSize) {
    this.poolSize = poolSize;
  }

  public void init() {
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
    if (ds != null) {
      try {
        ds.close();
      } catch (SQLException e) {
        LOGGER.trace("Unable to close datasource", e);
      }
    }
  }

  private void createDB() {
    LOGGER.info("Creating Storage DB at URL: {}", dbUrl);
    Flyway flyway = Flyway.configure().dataSource(dbUrl, user, password).load();
    flyway.migrate();
  }

  private void initDataSource() {
    ds.setUrl(dbUrl);
    ds.setUsername(user);
    ds.setPassword(password);
    ds.setMinIdle(5);
    ds.setMaxIdle(10);
    ds.setMaxOpenPreparedStatements(poolSize * 200);
  }

  private void insertMetacards(List<Metacard> metacards) throws IngestException {
    long insertTime = System.currentTimeMillis();

    try (Connection conn = ds.getConnection()) {
      for (Metacard metacard : metacards) {
        String encodedMetacard = getEncodedMetacard(metacard);
        try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
          ps.setString(1, metacard.getId());
          ps.setLong(2, insertTime);
          ps.setString(3, encodedMetacard);
          try {
            int numRecords = ps.executeUpdate();
            LOGGER.trace("Inserted {} records", numRecords);
          } catch (SQLException e) {
            if (e.getSQLState().contains("23000") || e.getSQLState().contains("23505")) {
              LOGGER.trace("Integrity constraint, attempting to update");
              updateMetacards(Collections.singletonList(metacard));
            } else {
              throw e;
            }
          }
        }
      }
      if (LOGGER.isTraceEnabled()) {
        long totalTime = System.currentTimeMillis() - insertTime;
        LOGGER.trace("Total time to insert records: {} ms", totalTime);
      }
    } catch (SQLException e) {
      LOGGER.debug("SQL Exception encountered while storing data", e);
      throw new IngestException("Unable to insert metadata to: " + dbUrl, e);
    }
  }

  private void updateMetacards(List<Metacard> metacards) throws IngestException {
    long insertTime = System.currentTimeMillis();

    try (Connection conn = ds.getConnection()) {
      for (Metacard metacard : metacards) {
        String encodedMetacard = getEncodedMetacard(metacard);
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_SQL)) {
          ps.setLong(1, insertTime);
          ps.setString(2, encodedMetacard);
          ps.setString(3, metacard.getId());
          int numRecords = ps.executeUpdate();
          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Updated: {} with {} records", metacard.getId(), numRecords);
          }
        }
      }
    } catch (SQLException e) {
      throw new IngestException("Unable to get DB connection: " + dbUrl, e);
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
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(getQuery(ids.size()))) {
      int index = 1;
      for (String id : ids) {
        LOGGER.trace("Setting query index[{}]: {}", index, id);
        ps.setString(index, id);
        index++;
      }
      rs = ps.executeQuery();
      while (rs.next()) {
        String id = rs.getString("ID");
        String metacardXml = rs.getString("METACARD_DATA");
        Metacard metacard = getMetacard(id, metacardXml);
        metacards.add(metacard);
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
