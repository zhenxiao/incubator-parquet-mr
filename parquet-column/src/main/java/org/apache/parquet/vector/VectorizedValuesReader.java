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
package org.apache.parquet.vector;

import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.values.ValuesReader;

import java.io.IOException;

public class VectorizedValuesReader extends ValuesReader {
  private ValuesReader valuesReader;
  private DataPage[] pages;

  public VectorizedValuesReader(ValuesReader valuesReader) {
    this.valuesReader = valuesReader;
  }

  public void setPages(DataPage[] pages) {
    this.pages = pages;
  }

  @Override
  public void initFromPage(int valueCount, byte[] page, int offset) throws IOException {
    valuesReader.initFromPage(valueCount, page, offset);
  }

  @Override
  public void skip() {
    valuesReader.skip();
  }

  @Override
  public void readVector(ColumnVector vector) {
    int totalValueCount = 0;
    for (DataPage page : pages) {
      totalValueCount += page.getValueCount();
    }
    vector.setNumberOfValues(totalValueCount);
    vector.setPages(this.pages);
    vector.setDecoder(valuesReader);
  }
}
