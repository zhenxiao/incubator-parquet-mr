package parquet.vector;

import parquet.column.page.DataPage;
import parquet.column.values.ValuesReader;

import java.io.IOException;

public class VectorizedValuesReader extends ValuesReader {
  private ValuesReader valuesReader;
  private DataPage[] pages;

  public VectorizedValuesReader(ValuesReader valuesReader) {
    this.valuesReader = valuesReader;
  }

  public void setPages(DataPage[] pages) {
    this.pages = pages;
  }

  @Override
  public void initFromPage(int valueCount, byte[] page, int offset) throws IOException {
    valuesReader.initFromPage(valueCount, page, offset);
  }

  @Override
  public void skip() {
    valuesReader.skip();
  }

  @Override
  public void readVector(ColumnVector vector) {
    int totalValueCount = 0;
    for (DataPage page : pages) {
      totalValueCount += page.getValueCount();
    }
    vector.setNumberOfValues(totalValueCount);
    vector.setPages(this.pages);
    vector.setDecoder(valuesReader);
  }
}
