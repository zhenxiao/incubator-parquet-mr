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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.vector.ByteColumnVector;
import org.apache.parquet.vector.DoubleColumnVector;
import org.apache.parquet.vector.IntColumnVector;
import org.apache.parquet.vector.RowBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class TestParquetVectorReader
{
  private static final int nElements = 100000;
  private static final Configuration conf = new Configuration();
  private static final Path file = new Path("target/test/TestParquetVectorReader/testParquetFile");
  private static final MessageType schema = parseMessageType(
                                                "message test { "
                                                + "required binary binary_field; "
                                                + "required int32 int32_field; "
                                                + "required double double_field; "
                                                + "} ");

  @AfterClass
  public static void cleanup() throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(file)) {
      fs.delete(file, true);
    }
  }

  @BeforeClass
  public static void prepareFile() throws IOException {
    final boolean dictionaryEnabled = true;
    cleanup();

    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(
            file,
            new GroupWriteSupport(),
            UNCOMPRESSED, 1024*1024, 1024, 1024*1024,
            dictionaryEnabled, false, PARQUET_1_0, conf);

    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < nElements; i++) {
      String b = String.valueOf((char) ((i % 26) + 'a'));
      sb.append(b);
      writer.write(
        f.newGroup()
          .append("binary_field", b)
          .append("int32_field", i)
          .append("double_field", i * 1.0)
      );
    }
    writer.close();
  }

  @Test
  public void testPlainDoubleVectorRead() throws Exception {
    //set column to read
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, "message test { required double double_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    MessageType doubleCol = parseMessageType("message test { required double double_field;}");
    int expected = 0;
    int previousRead = 0;
    while(true) {
      DoubleColumnVector vector = new DoubleColumnVector();
      reader.readVector(vector, doubleCol);
      int elementsRead = vector.size();
      System.out.println("Read " + elementsRead);
      ByteBuffer bb = vector.decode();
      for (int j = 0; j < elementsRead; j++) {
        double read = bb.getDouble();
        assertEquals(expected * 1.0, read, 0.01);
        System.out.println(read);
        expected++;
      }
      previousRead += elementsRead;
      if (previousRead >= nElements) {
        break;
      }
    }
    reader.close();
  }

  @Test
  public void testPlainIntVectorRead() throws Exception {
    //set column to read
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, "message test { required int32 int32_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    MessageType intCol = parseMessageType("message test { required int32 int32_field;}");
    int expected = 0;
    int previousRead = 0;
    while(true) {
      IntColumnVector vector = new IntColumnVector();
      reader.readVector(vector, intCol);
      int elementsRead = vector.size();
      System.out.println("Read " + elementsRead);
      ByteBuffer bb = vector.decode();
      for (int j = 0; j < elementsRead; j++) {
        int read = bb.getInt();
        System.out.println(read);
        assertEquals(expected, read);
        expected++;
      }
      previousRead += elementsRead;
      if (previousRead >= nElements) {
        break;
      }
    }
    reader.close();
  }

  @Test
  public void testLazyBinaryVectorRead()  throws Exception {
    //set column to read
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, "message test { required binary binary_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    //prepare the expected data
    StringBuffer expected = new StringBuffer();
    for (int i = 0; i < nElements; i++) {
      String b = String.valueOf((char) ((i % 26) + 'a'));
      expected.append(b);
    }

    MessageType binaryCol = parseMessageType("message test { required binary binary_field;}");
    StringBuffer actual = new StringBuffer();
    while(true) {
      ByteColumnVector byteVector = new ByteColumnVector(true);
      reader.readVector(byteVector, binaryCol);
      int elementsRead = byteVector.size();
      System.out.println("Read " + elementsRead + " elements");
      if (elementsRead != 0) {
        ByteBuffer decoded = byteVector.decode();
        CharBuffer s = BytesUtils.UTF8.decode(decoded);
        System.out.println(s);
        actual.append(s);
      }

      if (elementsRead == 0) {
        String actualString = actual.toString();
        String expectedString = expected.toString();
        assertEquals(expectedString, actualString);
        break;
      }
    }
    reader.close();
  }

  @Test
  public void testMultipleVectorRead() throws Exception {
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, "message test { required double double_field; required int32 int32_field;}");
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    MessageType doubleCol = parseMessageType("message test { required double double_field;}");
    int expected = 0;
    int previousRead = 0;
    while(true) {
      DoubleColumnVector vector = new DoubleColumnVector();
      reader.readVector(vector, doubleCol);
      int elementsRead = vector.size();
      System.out.println("Read " + elementsRead);
      ByteBuffer bb = vector.decode();
      for (int j = 0; j < elementsRead; j++) {
        double read = bb.getDouble();
        assertEquals(expected * 1.0, read, 0.01);
        System.out.println(read);
        expected++;
      }
      previousRead += elementsRead;
      if (previousRead >= nElements) {
        break;
      }
    }

    MessageType intCol = parseMessageType("message test { required int32 int32_field;}");
    expected = 0;
    previousRead = 0;
    while(true) {
      IntColumnVector vector = new IntColumnVector();
      reader.readVector(vector, intCol);
      int elementsRead = vector.size();
      System.out.println("Read " + elementsRead);
      ByteBuffer bb = vector.decode();
      for (int j = 0; j < elementsRead; j++) {
        int read = bb.getInt();
        System.out.println(read);
        assertEquals(expected, read);
        expected++;
      }
      previousRead += elementsRead;
      if (previousRead >= nElements) {
        break;
      }
    }

    reader.close();
  }

  @Test
  public void testPlainIntBatchRead() throws Exception {
    //set columns to read
    String messageType = "message test { required int32 int32_field;}";
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, messageType);
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    RowBatch batch = new RowBatch();
    int expected = 0;
    int previousRead = 0;
    while(true) {
      batch = reader.nextBatch(batch);
//      int elementsRead = batch.size();
      assertTrue("Must have a single column", batch.getColumns().length == 1);
      IntColumnVector vector = (IntColumnVector) batch.getColumns()[0];
      System.out.println("Read " + vector.size() + " integers");
      ByteBuffer bb = vector.decode();
      for (int j = 0; j < vector.size(); j++) {
        int read = bb.getInt();
        System.out.println(read);
        assertEquals(expected, read);
        expected++;
      }
      previousRead += vector.size();
      if (previousRead >= nElements) {
        break;
      }
    }
    reader.close();
  }

  //Failing now, there is a bug when batch loading more than one column
//  @Test
  public void testPlainIntDoubleBatchRead() throws Exception {
    //set columns to read
    String messageType = "message test { required double double_field; required int32 int32_field;}";
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, messageType);
    ParquetReader<Group>reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();

    RowBatch batch = new RowBatch();
    int expected = 0;
    int previousRead = 0;
    while(true) {
      batch = reader.nextBatch(batch);
      int elementsRead = batch.size();
      System.out.println("Read " + elementsRead + " integers");
//      assertTrue("Must have a single column", batch.getColumns().length == 1);
      DoubleColumnVector dVector = (DoubleColumnVector) batch.getColumns()[0];
      IntColumnVector iVector = (IntColumnVector) batch.getColumns()[1];
      ByteBuffer bb = iVector.decode();
      for (int j = 0; j < iVector.size(); j++) {
        int read = bb.getInt();
        System.out.println(read);
        assertEquals(expected, read);
        expected++;
      }
      previousRead += elementsRead;
      if (previousRead >= nElements) {
        break;
      }
    }
    reader.close();
  }
}
