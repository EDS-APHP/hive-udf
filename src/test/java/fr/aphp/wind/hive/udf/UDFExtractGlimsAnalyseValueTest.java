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
      "struct<type_calc:string,value_num_calc:double,unit_calc:string,value_text_calc:string>";

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
    for (Object in : ins)
      udtf.process(new Object[] {in});
    return out;
  }



  private static ConstantObjectInspector toConstantOI(final String text) {
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text(text));
  }

  private static Object toObject(final String text) {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector.create(text);
  }

  @Test
  public void howToManageNumeric() throws HiveException {
    final String VALUE = "9.9 mg/L";
    final String RANGE = "9\10";

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE)});
    assertEquals(
        this.STRUCT,
        oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".type_calc");
    final HivePath col2 = new HivePath(oi, ".value_num_calc");
    final HivePath col3 = new HivePath(oi, ".unit_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE));

    assertEquals(1, results.size());
    assert ("numeric".equals(col1.extract(results.get(0)).asString()));
    assert (9.9 == col2.extract(results.get(0)).asDouble());
    assertEquals("mg/L", col3.extract(results.get(0)).asString());

  }

  @Test
  public void howToManageDatetime() throws HiveException {
    final String VALUE = "01/01/1988 13:24";
    final String RANGE = "9\10";

    final UDFExtractGlimsAnalyseValue sut = new UDFExtractGlimsAnalyseValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI(RANGE)});
    assertEquals(this.STRUCT, oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".type_calc");
    final HivePath col2 = new HivePath(oi, ".value_num_calc");
    final HivePath col3 = new HivePath(oi, ".unit_calc");
    final HivePath col4 = new HivePath(oi, ".value_text_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE));

    assertEquals(1, results.size());
    assert ("datetime".equals(col1.extract(results.get(0)).asString()));
    // assertEquals(col2.extract(results.get(0)).asDouble(), null);
    assertEquals(null, col3.extract(results.get(0)).asString());
    assertEquals(VALUE, col4.extract(results.get(0)).asString());

  }
}
