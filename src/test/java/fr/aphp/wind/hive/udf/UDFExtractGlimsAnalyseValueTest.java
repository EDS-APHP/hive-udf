package fr.aphp.wind.hive.udf;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class UDFExtractGlimsAnalyseValueTest {

  private UDFExtractGlimsAnalyseValue udf;
  private Object STRUCT =
      "struct<value_type_calc:string,value_num_calc:double,value_text_calc:string,value_num_unit_calc:string,value_num_operator_calc:string,value_num_borne_inf_calc:double,value_num_borne_sup_calc:double,value_num_borne_calc:string>";

  @Before
  public void before() {
    udf = new UDFExtractGlimsAnalyseValue();
  }

  private static List<Object> evaluate(final GenericUDTF udtf, final Object... ins)
      throws HiveException {
    final List<Object> out = new ArrayList<>();
    udtf.setCollector(new Collector() {
      @Override
      public void collect(Object input) throws HiveException {
        out.add(input);
      }
    });
//    for (Object in : ins)
      udtf.process(new Object[] {ins[0],ins[1],ins[2]});
    return out;
  }



  private static ConstantObjectInspector toConstantOI(final String text) {
    if(text==null)
      return null;
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text(text));
  }

  private static Object toObject(final String text) {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector.create(text);
  }

  @Test
  public void howToManageNumeric() throws HiveException {
    final String VALUE = "9.9 mg/L";
    final String RANGE = "32.0\\36.0";

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".value_type_calc");
    final HivePath col2 = new HivePath(oi, ".value_num_calc");
    final HivePath col3 = new HivePath(oi, ".value_num_unit_calc");
    final HivePath col4 = new HivePath(oi, ".value_num_borne_inf_calc");
    final HivePath col5 = new HivePath(oi, ".value_num_borne_sup_calc");
    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject("apr"));

    assertEquals(1, results.size());
    assert ("numeric".equals(col1.extract(results.get(0)).asString()));
    assert (9.9 == col2.extract(results.get(0)).asDouble());
    assertEquals("mg/L", col3.extract(results.get(0)).asString());
    assertEquals(32.0, col4.extract(results.get(0)).asDouble(),0);
    assertEquals(36.0, col5.extract(results.get(0)).asDouble(),0);
    assertEquals("L", col6.extract(results.get(0)).asString());
    

  }
  
  @Test
  public void shouldReturnHigh() throws HiveException {
    final String VALUE = "40 mg/L";
    final String RANGE = "32.0\\36.0";

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());

    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject("apr"));

    assertEquals("H", col6.extract(results.get(0)).asString());
  }
  
  @Test
  public void shouldReturnLow() throws HiveException {
    final String VALUE = "29 mg/L";
    final String RANGE = ">= 32.0";

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());

    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject("apr"));

    assertEquals("L", col6.extract(results.get(0)).asString());
  }
  
  @Test
  public void shouldReturnNorm() throws HiveException {
    final String VALUE = "32 mg/L";
    final String RANGE = ">= 32.0";

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());

    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject("apr"));

    assertEquals("@", col6.extract(results.get(0)).asString());
  }
  
  @Test
  public void shouldReturnNormToo() throws HiveException {
    final String VALUE = "32";
    final String RANGE = ">= 32.0";

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());

    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject("apr"));

    assertEquals(null, col6.extract(results.get(0)).asString());
  }
  @Test
  public void shouldReturnVoid() throws HiveException {
    final String VALUE = "32 mg/L";
    final String RANGE = ">=32.0";//the range is not well defined then void

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());

    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject("apr"));

    assertEquals(null, col6.extract(results.get(0)).asString());
  }
  
  @Test
  public void shouldReturnVoidToo() throws HiveException {
    final String VALUE = null;
    final String RANGE = null;//the range is not well defined then void

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI(null)});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());

    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject(null));

    assertEquals(null, col6.extract(results.get(0)).asString());
  }
  
  @Test
  public void shouldReturnVoidTooToo() throws HiveException {
    final String VALUE = "";
    final String RANGE = "";//the range is not well defined then void

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("")});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());

    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject(""));

    assertEquals(null, col6.extract(results.get(0)).asString());
  }
  @Test
  public void shouldReturnSomething() throws HiveException {
    final String VALUE = "< 0.04 µg/l";
    final String RANGE = "\\0.15";//the range is not well defined then void

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("")});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());

    final HivePath col6 = new HivePath(oi, ".value_num_borne_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE),toObject(RANGE),toObject(""));

    assertEquals("@", col6.extract(results.get(0)).asString());
  }
  @Test
  public void howToManageDatetime() throws HiveException {
    final String VALUE = "01/01/1988 13:24";
    final String RANGE = "9\10";

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(this.STRUCT, oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".value_type_calc");
    final HivePath col2 = new HivePath(oi, ".value_num_calc");
    final HivePath col3 = new HivePath(oi, ".value_num_unit_calc");
    final HivePath col4 = new HivePath(oi, ".value_text_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE), toObject(RANGE), toObject("apr"));

    assertEquals(1, results.size());
    assert ("datetime".equals(col1.extract(results.get(0)).asString()));
  //  assert(Double.is(col2.extract(results.get(0)).asDouble()));
    assertEquals(null, col3.extract(results.get(0)).asString());
    assertEquals(VALUE, col4.extract(results.get(0)).asString());

  }
  
  @Test
  public void howToManageImg() throws HiveException {
    final String VALUE = "house.TIF";
    final String RANGE = null;

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(this.STRUCT, oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".value_type_calc");
    final HivePath col4 = new HivePath(oi, ".value_text_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE), toObject(RANGE), toObject("apr"));

    assertEquals(1, results.size());
    assert ("image".equals(col1.extract(results.get(0)).asString()));
  //  assert(Double.is(col2.extract(results.get(0)).asDouble()));
    assertEquals(VALUE, col4.extract(results.get(0)).asString());
  }
  
  @Test
  public void shouldNotReturnImage() throws HiveException {
    final String VALUE = "house.TIFO";
    final String RANGE = null;

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(this.STRUCT, oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".value_type_calc");
    final HivePath col4 = new HivePath(oi, ".value_text_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE), toObject(RANGE), toObject("apr"));

    assertEquals(1, results.size());
    assert ("text".equals(col1.extract(results.get(0)).asString()));
    assertEquals(VALUE, col4.extract(results.get(0)).asString());
  }
  
  @Test
  public void howToManageGptext() throws HiveException {
    final String VALUE = "hello {<_rpcCOL11A1} world!";
    final String RANGE = null;

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE), toConstantOI("apr")});
    assertEquals(this.STRUCT, oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".value_type_calc");
    final HivePath col4 = new HivePath(oi, ".value_text_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE), toObject(RANGE), toObject("apr"));

    assertEquals(1, results.size());
    assert ("gp_text".equals(col1.extract(results.get(0)).asString()));
    assertEquals("hello COL11A1 world!", col4.extract(results.get(0)).asString());
  }
  
}
