import com.func.udf.GenericUDFChineseAmount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class TestChineseFormat {
    @lombok.SneakyThrows
    public static void main(String[] args) {
        GenericUDFChineseAmount genericUDFChineseAmount = new GenericUDFChineseAmount();
        ObjectInspector inputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        ObjectInspector[] argsOI = {inputOI};
        genericUDFChineseAmount.initialize(argsOI);

        double testValue = 71667572899.576;
        GenericUDF.DeferredObject[] deferredObjects = {
                new GenericUDF.DeferredJavaObject(testValue)
        };
        Object result = genericUDFChineseAmount.evaluate(deferredObjects);
        System.out.println("转换结果: " + ((Text) result).toString());
    }
}
