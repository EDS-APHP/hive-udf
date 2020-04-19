package fr.aphp.wind.hive.udf;


import junit.framework.Assert;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

public class UDFLevenshteinDistanceTest {

    private UDFLevenshteinDistance udf;

    @Before
    public void before() {
        udf = new UDFLevenshteinDistance();
    }

    @Test(expected = UDFArgumentException.class)
    public void testInitializeWithOneArg() throws UDFArgumentException {
        udf.initialize(new ObjectInspector[0]);
    }

    @Test(expected = UDFArgumentException.class)
    public void testInitializeWithWrongTypeArgs() throws UDFArgumentException {
        ObjectInspector arg1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector arg2 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        udf.initialize(new ObjectInspector[]{arg1, arg2});
    }

    @Test
    public void testInitializeWithValidArgs() throws UDFArgumentException {
        ObjectInspector arg1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector arg2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        Assert.assertEquals(PrimitiveObjectInspectorFactory.writableStringObjectInspector,udf.initialize(new ObjectInspector[]{arg1, arg2})) ;
    }

    @Test
    public void testEvaluate()throws HiveException {

        //Phase 1 - Call initialize()
        ObjectInspector arg1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector arg2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(new ObjectInspector[]{arg1, arg2});

        //Phase 2 - Call evaluate()
        String c1 = "azerty";

        //We send the same String in order to have a distance = 0
        double result = (double) udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(c1),new GenericUDF.DeferredJavaObject(c1)});
        Assert.assertEquals((double)0, result);

        //For the second String we double the String in order to have a distance = string.length
        result = (double) udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(c1),
                new GenericUDF.DeferredJavaObject(c1.concat(c1))});
        Assert.assertEquals((double)c1.length(), result);
    }

}
