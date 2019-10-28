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
package ddf.catalog.transformer.xml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ddf.catalog.data.Metacard;
import ddf.catalog.data.MetacardType;
import ddf.catalog.source.solr.json.MetacardTypeMapperFactory;
import ddf.catalog.transform.CatalogTransformerException;
import ddf.catalog.transformer.api.PrintWriter;
import ddf.catalog.transformer.api.PrintWriterProvider;
import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.codice.ddf.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

public class TypedMetacardMarshallerImpl extends MetacardMarshallerImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(TypedMetacardMarshallerImpl.class);

  private static final ObjectMapper METACARD_TYPE_MAPPER =
      MetacardTypeMapperFactory.newObjectMapper();

  public TypedMetacardMarshallerImpl(Parser parser, PrintWriterProvider writerProvider) {
    super(parser, writerProvider);
  }

  @Override
  public String marshal(Metacard metacard, Map<String, Serializable> arguments)
      throws XmlPullParserException, IOException, CatalogTransformerException {
    PrintWriter writer = this.writerProvider.build(Metacard.class);

    if (arguments != null && arguments.get(OMIT_XML_DECL) != null) {
      Boolean omitXmlDec = Boolean.valueOf(String.valueOf(arguments.get(OMIT_XML_DECL)));
      if (omitXmlDec == null || !omitXmlDec) {
        writer.setRawValue(XML_DECL);
      }
    }

    writer.startNode("metacard");
    writeMetacardId(writer, metacard);
    writeSerializedType(writer, metacard);
    writeMetacard(writer, metacard);
    writer.endNode();
    return writer.makeString();
  }

  private void writeSerializedType(PrintWriter writer, Metacard metacard) {
    String serializedType = getSerializedType(metacard.getMetacardType());
    if (StringUtils.isNotBlank(serializedType)) {
      writer.startNode("type");
      writer.setValue(serializedType);
      writer.endNode(); // serializedType
    }
  }

  private String getSerializedType(MetacardType metacardType) {
    try {
      byte[] metacardTypeBytes = METACARD_TYPE_MAPPER.writeValueAsBytes(metacardType);
      return Base64.getEncoder().encodeToString(metacardTypeBytes);
    } catch (JsonProcessingException e) {
      LOGGER.info("Unable to serialize metacard type", e);
    }
    return null;
  }
}
