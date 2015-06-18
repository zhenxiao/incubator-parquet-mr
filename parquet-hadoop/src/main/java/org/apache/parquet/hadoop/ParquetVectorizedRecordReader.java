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
package org.apache.parquet.hadoop;

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.apache.parquet.Log;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.vector.ColumnVector;

/**
 * Reads the records from a block of a Parquet file
 *
 * @see ParquetInputFormat
 *
 * @author Zhenxiao Luo
 *
 * @param <T> type of the materialized records
 */
public class ParquetVectorizedRecordReader<T> extends RecordReader<Void, T>
{
    private static final Log LOG = Log.getLog(ParquetVectorizedRecordReader.class);
    private final InternalParquetRecordReader<T> internalReader;

    /**
     * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
     */
    public ParquetVectorizedRecordReader(ReadSupport<T> readSupport)
    {
        this(readSupport, FilterCompat.NOOP);
    }

    /**
     * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
     * @param filter for filtering individual records
     */
    public ParquetVectorizedRecordReader(ReadSupport<T> readSupport, Filter filter)
    {
        internalReader = new InternalParquetRecordReader<T>(readSupport, filter);
    }

    /**
     * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
     * @param filter for filtering individual records
     * @deprecated use {@link #ParquetVectorizedRecordReader(ReadSupport, Filter)}
     */
    @Deprecated
    public ParquetVectorizedRecordReader(ReadSupport<T> readSupport, UnboundRecordFilter filter)
    {
        this(readSupport, FilterCompat.get(filter));
    }

    /**
     * Does the reader have more rows available
     * @return true if there are more rows
     * @throws java.io.IOException
     */
    public boolean hasNext() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Read next batch of ColumnVector
     * ColumnVector holds the bytes of the vector
     * ColumnVector.decode() materializes the vector values
     * @param vector the vector to fill
     * @param column the requested column
     * @throws java.io.IOException
     */
    public void readVector(ColumnVector vector, MessageType column)
    {
        try {
            internalReader.nextBatch(vector, column);
        } catch (Exception e) {
            LOG.info("Get exception when reading batch: " + e.getMessage());
        }
    }

    /**
     * Get the row number of the row that will be returned by the following call to readVector()
     * @return the row number from 0 to the number of rows in the file
     * @throws java.io.IOException
     */
    public long getRowNumber() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Seek to a particular row number.
     */
    public void seekToRow(long rowCount) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
        internalReader.close();
    }

    @Override
    public Void getCurrentKey() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public T getCurrentValue() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return internalReader.getProgress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException
    {
        if (context instanceof TaskInputOutputContext<?, ?, ?, ?>) {
            BenchmarkCounter.initCounterFromContext((TaskInputOutputContext<?, ?, ?, ?>) context);
        }
        else {
            LOG.error("Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is "
                + context.getClass().getCanonicalName());
        }
        initializeInternalReader((ParquetInputSplit)inputSplit, ContextUtil.getConfiguration(context));
    }

    public void initialize(InputSplit inputSplit,
                            Configuration configuration,
                            Reporter reporter) throws IOException, InterruptedException {
        BenchmarkCounter.initCounterFromReporter(reporter,configuration);
        initializeInternalReader((ParquetInputSplit) inputSplit, configuration);
    }

    private void initializeInternalReader(ParquetInputSplit split, Configuration configuration) throws IOException
    {
        Path path = split.getPath();
        long[] rowGroupOffsets = split.getRowGroupOffsets();
        List<BlockMetaData> filteredBlocks;
        ParquetMetadata footer;
        // if task.side.metadata is set, rowGroupOffsets is null
        if (rowGroupOffsets == null) {
            // then we need to apply the predicate push down filter
            footer = readFooter(configuration, path, range(split.getStart(), split.getEnd()));
            MessageType fileSchema = footer.getFileMetaData().getSchema();
            Filter filter = getFilter(configuration);
            filteredBlocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
        } else {
            // otherwise we find the row groups that were selected on the client
            footer = readFooter(configuration, path, NO_FILTER);
            Set<Long> offsets = new HashSet<Long>();
            for (long offset : rowGroupOffsets) {
                offsets.add(offset);
            }
            filteredBlocks = new ArrayList<BlockMetaData>();
            for (BlockMetaData block : footer.getBlocks()) {
                if (offsets.contains(block.getStartingPos())) {
                    filteredBlocks.add(block);
                }
            }
            // verify we found them all
            if (filteredBlocks.size() != rowGroupOffsets.length) {
                long[] foundRowGroupOffsets = new long[footer.getBlocks().size()];
                for (int i = 0; i < foundRowGroupOffsets.length; i++) {
                    foundRowGroupOffsets[i] = footer.getBlocks().get(i).getStartingPos();
                }
            // this should never happen.
            // provide a good error message in case there's a bug
            throw new IllegalStateException(
                "All the offsets listed in the split should be found in the file."
                + " expected: " + Arrays.toString(rowGroupOffsets)
                + " found: " + filteredBlocks
                + " out of: " + Arrays.toString(foundRowGroupOffsets)
                + " in range " + split.getStart() + ", " + split.getEnd());
            }
        }
        MessageType fileSchema = footer.getFileMetaData().getSchema();
        Map<String, String> fileMetaData = footer.getFileMetaData().getKeyValueMetaData();
        internalReader.initialize(fileSchema, fileMetaData, path, filteredBlocks, configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }
}
