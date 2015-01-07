package parquet.vector;

import parquet.column.page.DataPage;
import parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DoubleColumnVector extends ColumnVector
{
  public double[] values;

  public DoubleColumnVector() {
    this(false);
  }

  public DoubleColumnVector(boolean isLazy) {
    super(double.class, isLazy);
    values = new double[MAX_VECTOR_LENGTH];
  }

  @Override
  public ByteBuffer decode() {
    //TODO how many rows to return / how to map the read pages to rows
    //TODO handle eager decoding
    //TODO allocator should allocate a bytebuffer for us
    ByteBuffer buf = ByteBuffer.allocate(size() * (Double.SIZE / Byte.SIZE));
    if (pages != null) {
      try {
        for (DataPage page : pages) {
          initDecoder(page);
          for (int i = 0; i < page.getValueCount(); i++) {
            buf.putDouble(decoder.readDouble());
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
