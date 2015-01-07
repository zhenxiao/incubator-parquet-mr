package parquet.vector;

import parquet.column.page.DataPage;
import parquet.io.ParquetDecodingException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ByteColumnVector extends ColumnVector
{
  public ByteColumnVector(boolean isLazy) {
    super(byte.class, isLazy);
  }

  @Override
  public ByteBuffer decode() {
    //TODO how many rows to return / how to map the read pages to rows
    //TODO handle eager decoding
    //TODO allocator should allocate a bytebuffer for us
    ByteArrayOutputStream decoded = new ByteArrayOutputStream();
    if (pages != null) {
      try {
        for (DataPage page : pages) {
          initDecoder(page);
          for (int i = 0; i < page.getValueCount(); i++) {
            decoded.write(decoder.readBytes().getBytes());
          }
        }
      } catch (IOException e) {
        throw new ParquetDecodingException(e);
      }
    }
    return ByteBuffer.wrap(decoded.toByteArray());
  }


}
