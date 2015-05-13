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
package org.apache.parquet.hadoop;

import static org.apache.parquet.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.GlobalMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.vector.ColumnVector;
import org.apache.parquet.vector.RowBatch;

/**
 * Read records from a Parquet file.
 * TODO: too many constructors (https://issues.apache.org/jira/browse/PARQUET-39)
 */
public class ParquetReader<T> implements Closeable {

  private final ReadSupport<T> readSupport;
  private final Configuration conf;
  private final ReadContext readContext;
  private final Iterator<Footer> footersIterator;
  private final GlobalMetaData globalMetaData;
  private final Filter filter;

  private InternalParquetRecordReader<T> reader;

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Path file, ReadSupport<T> readSupport) throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport) throws IOException {
    this(conf, file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @param unboundRecordFilter the filter to use to filter records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter) throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @param unboundRecordFilter the filter to use to filter records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter) throws IOException {
    this(conf, file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  private ParquetReader(Configuration conf,
                       Path file,
                       ReadSupport<T> readSupport,
                       Filter filter) throws IOException {
    this.readSupport = readSupport;
    this.filter = checkNotNull(filter, "filter");
    this.conf = conf;

    FileSystem fs = file.getFileSystem(conf);
    List<FileStatus> statuses = Arrays.asList(fs.listStatus(file, HiddenFileFilter.INSTANCE));
    List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
    this.footersIterator = footers.iterator();
    globalMetaData = ParquetFileWriter.getGlobalMetaData(footers);
    MessageType schema = globalMetaData.getSchema();
    Map<String, Set<String>> extraMetadata = globalMetaData.getKeyValueMetaData();
    readContext = readSupport.init(new InitContext(conf, extraMetadata, schema));
  }

  /**
   * @return the next record or null if finished
   * @throws IOException
   */
  public T read() throws IOException {
    try {
      if (reader != null && reader.nextKeyValue()) {
        return reader.getCurrentValue();
      } else {
        initReader();
        return reader == null ? null : read();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public void readVector(ColumnVector vector, MessageType column) throws IOException {
    try {
      if (reader != null && reader.nextBatch(vector, column)) {
        return;
      } else {
        initReader();
        if (reader == null) {
          return;
        } else {
          readVector(vector, column);
        }
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Reads the next batch of rows. The caller needs to check the returned batch size with {@link parquet.vector.RowBatch#size()}.
   * @param previous a row batch object to be reused by the reader if possible
   * @return the row batch that was read
   * @throws java.io.IOException
   */
   public RowBatch nextBatch(RowBatch previous) throws IOException {
     MessageType requestedSchema = readContext.getRequestedSchema();
     List<ColumnDescriptor> columns = requestedSchema.getColumns();
     int nColumns = columns.size();
     ColumnVector[] columnVectors;
     if (previous == null || previous.getColumns() == null) {
       previous = new RowBatch();
       columnVectors = new ColumnVector[nColumns];
       for (int i = 0; i < nColumns; i++) {
         ColumnVector columnVector = ColumnVector.createVector(columns.get(i));
         MessageType columnSchema = new MessageType(requestedSchema.getFieldName(i), requestedSchema.getType(i));
         readVector(columnVector, columnSchema);
         columnVectors[i] = columnVector;
       }
      } else {
       columnVectors = previous.getColumns();
       for (int i = 0; i < nColumns; i++) {
         ColumnVector columnVector = columnVectors[i];
         MessageType columnSchema = new MessageType(requestedSchema.getFieldName(i), requestedSchema.getType(i));
         readVector(columnVector, columnSchema);
       }
      }

     previous.setColumns(columnVectors);
     return previous;
   }

  private void initReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (footersIterator.hasNext()) {
      Footer footer = footersIterator.next();

      List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();

      MessageType fileSchema = footer.getParquetMetadata().getFileMetaData().getSchema();

      List<BlockMetaData> filteredBlocks = RowGroupFilter.filterRowGroups(
          filter, blocks, fileSchema);

      reader = new InternalParquetRecordReader<T>(readSupport, filter);
      reader.initialize(fileSchema,
          footer.getParquetMetadata().getFileMetaData(),
          footer.getFile(), filteredBlocks, conf);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  public static <T> Builder<T> builder(ReadSupport<T> readSupport, Path path) {
    return new Builder<T>(readSupport, path);
  }

  public static class Builder<T> {
    private final ReadSupport<T> readSupport;
    private final Path file;
    private Filter filter;
    protected Configuration conf;

    private Builder(ReadSupport<T> readSupport, Path path) {
      this.readSupport = checkNotNull(readSupport, "readSupport");
      this.file = checkNotNull(path, "path");
      this.conf = new Configuration();
      this.filter = FilterCompat.NOOP;
    }

    protected Builder(Path path) {
      this.readSupport = null;
      this.file = checkNotNull(path, "path");
      this.conf = new Configuration();
      this.filter = FilterCompat.NOOP;
    }

    public Builder<T> withConf(Configuration conf) {
      this.conf = checkNotNull(conf, "conf");
      return this;
    }

    public Builder<T> withFilter(Filter filter) {
      this.filter = checkNotNull(filter, "filter");
      return this;
    }

    protected ReadSupport<T> getReadSupport() {
      // if readSupport is null, the protected constructor must have been used
      Preconditions.checkArgument(readSupport != null,
          "[BUG] Classes that extend Builder should override getReadSupport()");
      return readSupport;
    }

    public ParquetReader<T> build() throws IOException {
      return new ParquetReader<T>(conf, file, getReadSupport(), filter);
    }
  }
}
