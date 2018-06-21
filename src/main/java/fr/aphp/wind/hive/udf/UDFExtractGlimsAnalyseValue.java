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

public class UDFExtractGlimsAnalyseValue extends GenericUDTF {

  private static final Integer OUT_COLS = 8;
  // the output columns size
  private transient Object forwardColObj[] = new Object[OUT_COLS];

  private transient ObjectInspector[] inputOIs;
  private Pattern typeNumericPattern;
  private Pattern typeDatetimePattern;
  private Pattern typeImagePattern;
  private Pattern typeDatePattern;
  private Pattern typeHourPattern;
  private Pattern typeXmlPattern;

  private GlimsGptext gg;

  /**
   *
   * @param argOIs check the argument is valid.
   * @return the output column structure.
   * @throws UDFArgumentException
   */
  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

    if (argOIs.length != 3
        || (argOIs[0] != null && (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
            || !argOIs[0].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)))) {
      throw new UDFArgumentException("split_url only take one argument with type of string");
    }

    inputOIs = argOIs;
    List<String> outFieldNames = new ArrayList<String>();
    List<ObjectInspector> outFieldOIs = new ArrayList<ObjectInspector>();

    outFieldNames.add("value_type_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldNames.add("value_num_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    outFieldNames.add("value_text_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldNames.add("value_num_unit_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldNames.add("value_num_operator_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldNames.add("value_num_borne_inf_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    outFieldNames.add("value_num_borne_sup_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    outFieldNames.add("value_num_borne_calc");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);



    typeNumericPattern = Pattern.compile("^[<> ]*([-]?\\d{1,8}\\.?\\d{0,5}) ([^\\d]+)$");
    typeDatetimePattern = Pattern.compile("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{1,4} [0-9]{2}:[0-9]{2}$");
    typeImagePattern = Pattern.compile("(?i)(?:\\.png$)|(?:\\.tif$)|(?:\\.bmp$)");
    typeDatePattern = Pattern.compile("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{1,4}$");
    typeHourPattern = Pattern.compile("^[0-9]{2}h[0-9]{2}min$");
    typeXmlPattern = Pattern.compile("<\\?xml");

    gg = new GlimsGptext("glims_ref_gp_text.csv");


    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  public Reader getReader(String relativePath) {

    try {

      return new FileReader(relativePath);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

  }



  private String inferType(String value) {

    if (value == null) {
      return "text";
    }
    if (gg.getTypeGptextPattern().matcher(value).find()) {
      return "gp_text";
    }
    if (typeImagePattern.matcher(value).find()) {
      return "image";
    }
    if (typeDatetimePattern.matcher(value).find()) {
      return "datetime";
    }
    if (typeDatePattern.matcher(value).find()) {
      return "date";
    }
    if (typeHourPattern.matcher(value).find()) {
      return "hour";
    }
    if (typeNumericPattern.matcher(value).matches()) {
      return "numeric";
    }
    if (typeXmlPattern.matcher(value).find()) {
      return "xml";
    }

    return "text";
  }


  private Double[] explodeRange(String range) {

    String lower = null;
    String higher = null;
    if (range == null) {
      return prepareRange(lower, higher);
    }


    String doubleRange = "(-?[0-9]+\\.?[0-9]*)\\\\(-?[0-9]+\\.?[0-9]*)";
    Matcher mdrange = getMatcher(doubleRange, range);
    if (mdrange.matches()) {
      lower = mdrange.group(1);
      higher = mdrange.group(2);
      return prepareRange(lower, higher);
    }

    String lowRange = "^<[=]? (-?[0-9]+\\.?[0-9]*)";
    Matcher mlrange = getMatcher(lowRange, range);
    if (mlrange.matches()) {
      lower = mlrange.group(1);
      return prepareRange(lower, higher);
    }

    String lowRangeBis = "(-?[0-9]+\\.?[0-9]*)\\\\";
    Matcher mlrangebis = getMatcher(lowRangeBis, range);
    if (mlrangebis.matches()) {
      higher = mlrangebis.group(1);
      return prepareRange(lower, higher);
    }

    String highRange = "^>[=]? (-?[0-9]+\\.?[0-9]*)";
    Matcher mhrange = getMatcher(highRange, range);
    if (mhrange.matches()) {
      higher = mhrange.group(1);
      return prepareRange(lower, higher);
    }

    String highRangeBis = "\\\\(-?[0-9]+\\.?[0-9]*)";
    Matcher mhrangebis = getMatcher(highRangeBis, range);
    if (mhrangebis.matches()) {
      lower = mhrangebis.group(1);
      return prepareRange(lower, higher);
    }
    String doubleRangeBis = "(-?[0-9]+\\.?[0-9]*)- \\+?([0-9]+\\.?[0-9]*)";
    Matcher drangebis = getMatcher(doubleRangeBis, range);
    if (drangebis.matches()) {
      lower = drangebis.group(1);
      higher = drangebis.group(2);
      return prepareRange(lower, higher);
    }
    return prepareRange(lower, higher);
  }

  private Double[] prepareRange(String lower, String higher) {
    Double lowerRst = null;
    Double higherRst = null;
    if (lower != null) {
      lowerRst = Double.parseDouble(lower);
    }
    if (higher != null) {
      higherRst = Double.parseDouble(higher);
    }
    Double[] rst = new Double[] {lowerRst, higherRst};

    return rst;
  }

  private Matcher getMatcher(String pattern, String source) {
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(source);
    return m;
  }

  private String getBorne(Double value, Double low, Double high) {
    String lowValue = "L";
    String highValue = "H";
    String normValue = "@";
    if (low == null && high == null) {
      return null;
    }
    if (low != null && high == null) {
      if (value > low) {
        return highValue;
      } else {
        return normValue;
      }
    }
    if (low == null && high != null) {
      if (value < high) {
        return lowValue;
      } else {
        return normValue;
      }
    }
    if (low != null && high != null) {
      if (value <= low) {
        return lowValue;
      } else if (value >= high) {
        return highValue;
      } else {
        return normValue;
      }
    }
    return null;
  }

  @Override
  public void process(Object[] records) throws HiveException {
    // need OI to convert data type to get java type
    String range = null;
    String source = null;
    String value = null;

    if (inputOIs[0] != null) {
      value = ((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(records[0]);
    }

    if (inputOIs[1] != null) {
      range = ((StringObjectInspector) inputOIs[1]).getPrimitiveJavaObject(records[1]);
    }

    if (inputOIs[2] != null) {
      source = ((StringObjectInspector) inputOIs[2]).getPrimitiveJavaObject(records[2]);
    }

    String value_type_calc = inferType(value);
    Double value_num_calc = null;
    String value_text_calc = null;
    String value_num_unit_calc = null;
    String value_num_operator_calc = null;
    Double value_num_borne_inf_calc = null;
    Double value_num_borne_sup_calc = null;
    String value_num_borne_calc = null;


    if ("datetime".equals(value_type_calc)) {
      value_text_calc = value;
    } else if ("numeric".equals(value_type_calc)) {
      String[] spl = explodeNumericValue(value);
      value_num_calc = Double.parseDouble(spl[0]);
      value_num_unit_calc = spl[1];
      Double[] rspl = explodeRange(range);
      value_num_borne_inf_calc = rspl[0];
      value_num_borne_sup_calc = rspl[1];
      value_num_borne_calc =
          getBorne(value_num_calc, value_num_borne_inf_calc, value_num_borne_sup_calc);

    } else if ("image".equals(value_type_calc)) {
      value_text_calc = value;
    } else if ("gp_text".equals(value_type_calc)) {
      value_text_calc = gg.populateGptext(value, source);
    } else { // text, xml, hour
      value_text_calc = value;
    }
    forwardColObj[0] = value_type_calc;
    forwardColObj[1] = value_num_calc;
    forwardColObj[2] = value_text_calc;
    forwardColObj[3] = value_num_unit_calc;
    forwardColObj[4] = value_num_operator_calc;
    forwardColObj[5] = value_num_borne_inf_calc;
    forwardColObj[6] = value_num_borne_sup_calc;
    forwardColObj[7] = value_num_borne_calc;



    // output a row with two column
    forward(forwardColObj);
  }

  private String[] explodeNumericValue(String value) {
    Matcher a = this.typeNumericPattern.matcher(value);
    a.matches();
    return new String[] {a.group(1), a.group(2)};

  }

  @Override
  public void close() throws HiveException {

  }
}
