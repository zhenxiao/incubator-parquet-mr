/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.vector;

import parquet.column.page.DataPage;
import parquet.column.page.DataPageV1;
import parquet.column.page.DataPageV2;
import parquet.column.values.ValuesReader;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ColumnVector
{
  public static int MAX_VECTOR_LENGTH = 100; //TODO currently in terms of pages, will probably change

  protected boolean[] isNull;
  protected Class valueType;
  protected boolean isLazy;
  protected int numValues;
  protected DataPage[] pages;

  //for lazy decoding
  protected ValuesReader decoder;

  public ColumnVector(Class valueType, boolean isLazy) {
    this.isNull = new boolean[MAX_VECTOR_LENGTH];
    this.valueType = valueType;
    this.isLazy = isLazy;
  }

  /**
   * @return the type of this vector
   */
  public Class getType(){
    return valueType;
  }

  /**
   * Decodes the values in this column vector
   * and returns the decoded values in a ByteBuffer
   */
  abstract public ByteBuffer decode();

  /**
   * @return the number of values in this column vector
   */
  public int size() {
    return numValues;
  }

  /**
   * @return whether this is a lazy column vector
   */
  public boolean isLazy(){
    return isLazy;
  }

  /**
   * @return the isNull array
   */
  public boolean[] getIsNull(){
    return isNull;
  }

  void setNumberOfValues(int numValues)
  {
    this.numValues = numValues;
  }

  void setPages(DataPage[] pages) {
    this.pages = pages;
  }

  void setDecoder(ValuesReader decoder) {
    this.decoder = decoder;
  }

  protected void initDecoder(DataPage page) throws IOException {
    if (page instanceof DataPageV1)
      decoder.initFromPage(page.getValueCount(), ((DataPageV1)page).getBytes().toByteArray(), 0);
    else
      decoder.initFromPage(page.getValueCount(), ((DataPageV2)page).getData().toByteArray(), 0);
  }
}
