package fr.aphp.wind.hive.udf;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class UDFExtractDomainMethod extends GenericUDTF {

  private static final Integer OUT_COLS = 2;
  // the output columns size
  private transient Object forwardColObj[] = new Object[OUT_COLS];

  private transient ObjectInspector[] inputOIs;

  /**
   *
   * @param argOIs check the argument is valid.
   * @return the output column structure.
   * @throws UDFArgumentException
   */
  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

    if (argOIs.length != 1 || argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        || !argOIs[0].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
      throw new UDFArgumentException("split_url only take one argument with type of string");
    }

    inputOIs = argOIs;
    List<String> outFieldNames = new ArrayList<String>();
    List<ObjectInspector> outFieldOIs = new ArrayList<ObjectInspector>();
    outFieldNames.add("host");
    outFieldNames.add("method");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    // writableStringObjectInspector correspond to hadoop.io.Text
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  @Override
  public void process(Object[] records) throws HiveException {
    try {
      // need OI to convert data type to get java type
      String inUrl = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(records[0]);

      URI uri = new URI(inUrl);
      forwardColObj[0] = uri.getHost();
      forwardColObj[1] = uri.getRawPath();

      // output a row with two column
      forward(forwardColObj);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws HiveException {

  }
}
