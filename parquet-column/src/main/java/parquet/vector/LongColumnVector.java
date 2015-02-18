package parquet.vector;

import parquet.column.page.DataPage;
import parquet.io.ParquetDecodingException;

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
