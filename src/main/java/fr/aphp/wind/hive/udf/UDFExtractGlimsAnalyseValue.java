package fr.aphp.wind.hive.udf;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class UDFExtractGlimsAnalyseValue extends GenericUDTF {

  private static final Integer OUT_COLS = 4;
  // the output columns size
  private transient Object forwardColObj[] = new Object[OUT_COLS];

  private transient ObjectInspector[] inputOIs;
  private Pattern typeNumericPattern;
  private Pattern typeDatetimePattern;

  /**
   *
   * @param argOIs check the argument is valid.
   * @return the output column structure.
   * @throws UDFArgumentException
   */
  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

    if (argOIs.length != 2 || argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        || !argOIs[0].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
      throw new UDFArgumentException("split_url only take one argument with type of string");
    }

    inputOIs = argOIs;
    List<String> outFieldNames = new ArrayList<String>();
    List<ObjectInspector> outFieldOIs = new ArrayList<ObjectInspector>();
    
    outFieldNames.add("type_calc");
    outFieldNames.add("value_num_calc");
    outFieldNames.add("unit_calc");
    outFieldNames.add("value_text_calc");
    
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    // writableStringObjectInspector correspond to hadoop.io.Text
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
   
    
     typeNumericPattern = Pattern.compile("^[<> -]*\\d{1,8}\\.?\\d{0,5}");
     typeDatetimePattern = Pattern.compile("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{1,4} [0-9]{2}:[0-9]{2}$");
     

    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  final

  private String inferType(String value) {
    
    if (typeDatetimePattern.matcher(value).find()) {
      return "datetime";
    }
    if (typeNumericPattern.matcher(value).find()) {
      return "numeric";
    }

    return "text";
  }

  @Override
  public void process(Object[] records) throws HiveException {
    // need OI to convert data type to get java type
    String value = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(records[0]);
    String range = ((StringObjectInspector) inputOIs[1]).getPrimitiveJavaObject(records[0]);


    String type_calc = inferType(value);
    Double value_num_calc  = null;
    String unit_calc = null;
    String value_text_calc = null;
    
    if ("datetime".equals(type_calc)) {
      value_text_calc = value;
    }
    if ("numeric".equals(type_calc)) {
      String[] spl = value.split(" ");
      value_num_calc= Double.parseDouble(spl[0]);
      unit_calc = spl[1];
    }
    forwardColObj[0] = type_calc;
    forwardColObj[1] = value_num_calc;
    forwardColObj[2] = unit_calc;
    forwardColObj[3] = value_text_calc;
    

    // output a row with two column
    forward(forwardColObj);
  }

  @Override
  public void close() throws HiveException {

  }
}
