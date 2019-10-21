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
package org.codice.ddf.commands.solr;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ddf.catalog.CatalogFramework;
import ddf.catalog.data.impl.AttributeImpl;
import ddf.catalog.data.impl.MetacardImpl;
import ddf.catalog.data.types.Core;
import ddf.catalog.operation.CreateRequest;
import ddf.catalog.operation.CreateResponse;
import ddf.catalog.source.solr.SolrMetacardClientImpl;
import ddf.security.Subject;
import java.util.concurrent.Callable;
import org.apache.shiro.util.ThreadContext;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.codice.ddf.security.common.Security;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReindexCommandTest extends SolrCommandTest {

  @Test(expected = IllegalArgumentException.class)
  public void testNoArgReindex() throws Exception {
    ReindexCommand reindexCommand = new ReindexCommand();
    reindexCommand.execute();
  }

  @Test
  public void testReindex() throws Exception {
    ThreadContext.bind(mock(Subject.class));

    CloudSolrClient cloudClient = mock(CloudSolrClient.class);
    SolrPingResponse pingResponse = mock(SolrPingResponse.class);
    NamedList<Object> pingStatus = new NamedList<>();
    pingStatus.add("status", "OK");
    when(pingResponse.getResponse()).thenReturn(pingStatus);
    when(cloudClient.ping()).thenReturn(pingResponse);

    QueryResponse hitCountResponse = mock(QueryResponse.class);
    SolrDocumentList hitCountResults = mock(SolrDocumentList.class);
    when(hitCountResults.getNumFound()).thenReturn(1L);
    when(hitCountResponse.getResults()).thenReturn(hitCountResults);

    SolrDocument doc = new SolrDocument();
    doc.put("id_txt", "1234");
    SolrDocumentList dataDocumentList = new SolrDocumentList();
    dataDocumentList.add(doc);
    dataDocumentList.setNumFound(1L);
    QueryResponse dataResponse = mock(QueryResponse.class);
    when(dataResponse.getResults()).thenReturn(dataDocumentList);
    when(dataResponse.getNextCursorMark()).thenReturn("cursor1234");

    SolrDocumentList emptyDocList = new SolrDocumentList();
    dataDocumentList.add(doc);
    QueryResponse emptyResponse = mock(QueryResponse.class);
    when(emptyResponse.getResults()).thenReturn(emptyDocList);
    when(cloudClient.query(any(String.class), any()))
        .thenReturn(hitCountResponse, dataResponse, emptyResponse);

    SolrMetacardClientImpl solrMetacardClient = mock(SolrMetacardClientImpl.class);
    when(solrMetacardClient.createMetacard(any())).thenReturn(getTestMetacard());

    CreateResponse createResponse = mock(CreateResponse.class);
    CatalogFramework catalogFramework = mock(CatalogFramework.class);
    when(catalogFramework.create(any(CreateRequest.class))).thenReturn(createResponse);

    Security security = mock(Security.class);
    Subject subject = mock(Subject.class);
    when(security.runAsAdmin(any())).thenReturn(subject);
    when(subject.execute(any(Callable.class)))
        .thenAnswer(c -> ((Callable) c.getArguments()[0]).call());

    ReindexCommand command = new ReindexCommand();
    command.setSolrjClient(cloudClient);
    command.setMetacardClient(solrMetacardClient);
    command.setNumThread(1);
    command.setSourceCollection("catalog");
    command.setSourceSolrHost("http://localhost:8994/solr");
    command.setCatalogFramework(catalogFramework);
    command.setSecurity(security);
    command.execute();

    verify(catalogFramework, times(1)).create(any(CreateRequest.class));
  }

  private MetacardImpl getTestMetacard() {
    MetacardImpl metacard = new MetacardImpl();
    metacard.setAttribute(new AttributeImpl(Core.TITLE, "Test Card"));
    return metacard;
  }
}
