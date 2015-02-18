package parquet.vector;

import parquet.column.page.DataPage;
import parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FloatColumnVector extends ColumnVector
{
  public float[] values;

  public FloatColumnVector() {
    this(false);
  }

  public FloatColumnVector(boolean isLazy) {
    super(double.class, isLazy);
    values = new float[DEFAULT_VECTOR_LENGTH];
  }

  @Override
  public ByteBuffer decode() {
    //TODO how many rows to return / how to map the read pages to rows
    //TODO handle eager decoding
    //TODO allocator should allocate a bytebuffer for us
    ByteBuffer buf = ByteBuffer.allocate(size() * (Float.SIZE / Byte.SIZE));
    if (pages != null) {
      try {
        for (DataPage page : pages) {
          initDecoder(page);
          for (int i = 0; i < page.getValueCount(); i++) {
            buf.putFloat(decoder.readFloat());
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
