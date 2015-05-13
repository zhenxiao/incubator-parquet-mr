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
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LongColumnVector extends ColumnVector
{
  public long[] values;

  public LongColumnVector() {
    this(false);
  }

  public LongColumnVector(boolean isLazy) {
    super(int.class, isLazy);
    values = new long[DEFAULT_VECTOR_LENGTH];
  }

  public ByteBuffer decode() {
    //TODO how many rows to return / how to map the read pages to rows
    //TODO handle eager decoding
    //TODO allocator should allocate a bytebuffer for us
    ByteBuffer buf = ByteBuffer.allocate(size() * (Long.SIZE / Byte.SIZE));
    if (pages != null) {
      try {
        for (DataPage page : pages) {
          initDecoder(page);
          for (int i = 0; i < page.getValueCount(); i++) {
            buf.putLong(decoder.readLong());
          }
        }
      } catch (IOException e) {
        throw new ParquetDecodingException(e);
      }
    }
    buf.flip();
    return buf;
  }
}
