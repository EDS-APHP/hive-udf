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

public class UDFExtractGlimsGptextValueTest {

  private UDFExtractGlimsGptextValue udf;
  private Object STRUCT = "struct<value_gptext_calc:string>";

  @Before
  public void before() {
    udf = new UDFExtractGlimsGptextValue();
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
    
    udtf.process(new Object[] {ins[0], ins[1]});
    return out;
  }



  private static ConstantObjectInspector toConstantOI(final String text) {
    if (text == null)
      return null;
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text(text));
  }

  private static Object toObject(final String text) {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector.create(text);
  }


  @Test
  public void howToManageGptext() throws HiveException {
    final String VALUE = "hello {<_rpcCOL11A1} world!";
    final String RANGE = null;

    final UDFExtractGlimsGptextValue sut = new UDFExtractGlimsGptextValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI("apr")});
    assertEquals(this.STRUCT, oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".value_gptext_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE), toObject("apr"));

    assertEquals(1, results.size());
    assertEquals("hello COL11A1 world!", col1.extract(results.get(0)).asString());
  }

  @Test
  public void howToManageGptextRecurs() throws HiveException {
    final String VALUE = "hello {<test_recursiv} world!";
    final String RANGE = null;

    final UDFExtractGlimsGptextValue sut = new UDFExtractGlimsGptextValue();
    final StructObjectInspector oi =
        sut.initialize(new ObjectInspector[] {toConstantOI(VALUE), toConstantOI("psl")});
    assertEquals(this.STRUCT, oi.getTypeName());


    final HivePath col1 = new HivePath(oi, ".value_gptext_calc");
    final List<Object> results = evaluate(sut, toObject(VALUE), toObject("psl"));

    assertEquals(1, results.size());
    assertEquals("hello hello\nworld world!", col1.extract(results.get(0)).asString());
  }
}
