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

public class UDFExtractDOmainMethodTest {

  private UDFExtractDomainMethod udf;

  @Before
  public void before() {
    udf = new UDFExtractDomainMethod();
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

  private static final String TEST_JSON = "http://localhost/toto";

  private static ConstantObjectInspector toConstantOI(final String text) {
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text(text));
  }

  private static Object toObject(final String text) {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector.create(text);
  }

  @Test
  public void testMultiColumn() throws HiveException {
    final UDFExtractDomainMethod sut = new UDFExtractDomainMethod();
    final StructObjectInspector oi = sut.initialize(
        new ObjectInspector[] {
            toConstantOI(
               this.TEST_JSON)
            });
    assertEquals("struct<host:string,method:string>", oi.getTypeName());
    
    
    final HivePath namePath = new HivePath(oi, ".host");
    final HivePath methodPath = new HivePath(oi, ".method");
    final List<Object> results = evaluate(sut, toObject(TEST_JSON));
    
    assertEquals(1, results.size());
    assertEquals("localhost",namePath.extract(results.get(0)).asString());
    assertEquals("/toto",methodPath.extract(results.get(0)).asString());


  }

}
