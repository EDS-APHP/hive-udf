package fr.aphp.wind.hive.udf;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.derby.impl.sql.catalog.SYSROUTINEPERMSRowFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class UDFExtractGlimsGptextValue extends GenericUDTF {

  private static final Integer OUT_COLS = 1;
  // the output columns size
  private transient Object forwardColObj[] = new Object[OUT_COLS];

  private transient ObjectInspector[] inputOIs;
  GlimsGptext gg;


  /**
   *
   * @param argOIs check the argument is valid.
   * @return the output column structure.
   * @throws UDFArgumentException
   */
  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

    if (argOIs.length != 2
        || (argOIs[0] != null && (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
            || !argOIs[0].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)))) {
      throw new UDFArgumentException("ExtractGptext only take two arguments with types of string");
    }

    inputOIs = argOIs;
    List<String> outFieldNames = new ArrayList<String>();
    List<ObjectInspector> outFieldOIs = new ArrayList<ObjectInspector>();

    outFieldNames.add("value_gptext_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    gg = new GlimsGptext("glims_ref_gp_text.csv");

    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  @Override
  public void process(Object[] records) throws HiveException {
    // need OI to convert data type to get java type
    String value = null;
    String source = null;

    if (inputOIs[0] != null) {
      value = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(records[0]);
    }

    if (inputOIs[1] != null) {
      source = ((StringObjectInspector) inputOIs[1]).getPrimitiveJavaObject(records[1]);
    }

    String valueGptextCalc = null;

    valueGptextCalc = gg.populateGptext(value, source);

    forwardColObj[0] = valueGptextCalc;

    // output a row with two column
    forward(forwardColObj);
  }


  @Override
  public void close() throws HiveException {

  }
}
