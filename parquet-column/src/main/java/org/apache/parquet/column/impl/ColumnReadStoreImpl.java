/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.impl;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.vector.VectorizedColumnReaderImpl;

/**
 * Implementation of the ColumnReadStore
 *
 * Initializes individual columns based on schema and converter
 *
 * @author Julien Le Dem
 *
 */
public class ColumnReadStoreImpl implements ColumnReadStore {

  private final PageReadStore pageReadStore;
  private final GroupConverter recordConverter;
  private final MessageType schema;

  /**
   * @param pageReadStore uderlying page storage
   * @param recordConverter the user provided converter to materialize records
   * @param schema the schema we are reading
   */
  public ColumnReadStoreImpl(PageReadStore pageReadStore, GroupConverter recordConverter, MessageType schema) {
    super();
    this.pageReadStore = pageReadStore;
    this.recordConverter = recordConverter;
    this.schema = schema;
  }

  @Override
  public ColumnReader getColumnReader(ColumnDescriptor path) {
    return newMemColumnReader(path, pageReadStore.getPageReader(path));
  }

  //FIXME plug in the new vector reader for testing
  // similar to hive, vectorized reads should be enabled through configuration (false by default)
  public static boolean VECTORIZATION_ENABLED = true;
  private ColumnReader newMemColumnReader(ColumnDescriptor path, PageReader pageReader) {
    PrimitiveConverter converter = getPrimitiveConverter(path);
    if (VECTORIZATION_ENABLED)
      return new VectorizedColumnReaderImpl(path, pageReader, converter);
    else
      return new ColumnReaderImpl(path, pageReader, converter);
  }

  private PrimitiveConverter getPrimitiveConverter(ColumnDescriptor path) {
    Type currentType = schema;
    Converter currentConverter = recordConverter;
    for (String fieldName : path.getPath()) {
      final GroupType groupType = currentType.asGroupType();
      int fieldIndex = groupType.getFieldIndex(fieldName);
      currentType = groupType.getType(fieldName);
      currentConverter = currentConverter.asGroupConverter().getConverter(fieldIndex);
    }
    PrimitiveConverter converter = currentConverter.asPrimitiveConverter();
    return converter;
  }

}
