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
package parquet.hadoop.vector;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ColumnVector
{
    public enum VectorType
    {
        BOOLEAN_VECTOR,
        LONG_VECTOR,
        INT_VECTOR,
        FLOAT_VECTOR,
        DOUBLE_VECTOR,
        BINARY_VECTOR
    }

    /**
     * @return whether is null values
     */
    boolean[] isNull();
    
    /**
     * @return Vector Type, long, double, etc
     */
    VectorType getValueType();
    
    /**
     * @return ByteBuffer wrapped primitive arrays
     */
    ByteBuffer decode();

    /**
     * @return the number of values in this column vector
     */
    long getLength();

    /**
     * @return max number of values in this column vector
     */
    long getMaxLength();

    /**
     * @return whether it is a lazy column vector
     */
    boolean isLazyVector();

    /**
     * @param whether it is a lazy column vector
     */
    void setLazyVector(boolean isLazyVector);
}
