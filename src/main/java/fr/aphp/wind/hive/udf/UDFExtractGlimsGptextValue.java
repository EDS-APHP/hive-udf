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
  private Pattern typeGptextPattern;

  private HashMap<String, String> map;

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

    typeGptextPattern = Pattern.compile("\\{<[\\+]?([\\w_]+?)\\}");

    map = new HashMap<String, String>();
    populateMap("glims_ref_gp_text.csv");


    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  public Reader getReader(String relativePath) {
    try {

      return new FileReader(relativePath);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

  }

  private void populateMap(String path) {

    CsvParserSettings parserSettings = new CsvParserSettings();
    // the file used in the example uses '\n' as the line separator sequence.
    // the line separator sequence is defined here to ensure systems such as MacOS and Windows
    // are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
    parserSettings.getFormat().setLineSeparator("\n");
    parserSettings.setHeaderExtractionEnabled(false);
    parserSettings.setQuoteDetectionEnabled(true);
    parserSettings.getFormat().setCharToEscapeQuoteEscaping('"');
    parserSettings.getFormat().setDelimiter(",".charAt(0));
    parserSettings.setMaxCharsPerColumn(-1);

    // creates a CSV parser
    CsvParser parser = new CsvParser(parserSettings);
    parser.beginParsing(getReader(path));

    String[] row;
    while ((row = parser.parseNext()) != null) {
      map.put(row[0].toLowerCase(), row[1]);
    }

    // The resources are closed automatically when the end of the input is reached,
    // or when an error happens, but you can call stopParsing() at any time.

    // You only need to use this if you are not parsing the entire content.
    // But it doesn't hurt if you call it anyway.
    parser.stopParsing();
  }


  public String populateGptext(String s, String source) {
    if (s == null) {
      return null;
    }

    Matcher m = this.typeGptextPattern.matcher(s);
    LinkedList<String> ret = new LinkedList<String>();
    int previousBegin = 0;
    while (m.find()) {
      ret.add(s.substring(previousBegin, m.start()));
      ret.add(fetchGptext(m, source));
      previousBegin = m.end();
    }
    if (previousBegin != s.length()) {
      ret.add(s.substring(previousBegin, s.length()));
    }
    if (previousBegin == 0)
      return ret.get(0);

    return populateGptext(String.join("", ret), source);
  }

  private String fetchGptext(Matcher match, String source) {
    String value = source + match.group(1).toLowerCase();
    String key = map.get(value);
    return key;
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

    valueGptextCalc = populateGptext(value, source);

    forwardColObj[0] = valueGptextCalc;

    // output a row with two column
    forward(forwardColObj);
  }


  @Override
  public void close() throws HiveException {

  }
}
