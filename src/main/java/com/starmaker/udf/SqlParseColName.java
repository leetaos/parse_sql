package com.starmaker.udf;


import com.starmaker.util.SqlParseUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.*;


public class SqlParseColName extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        // check args
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("Check Args Length Error!!!");
        }

        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(0, "Check Args Type Error!!!");
        }

        // 返回字符串数组类型
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        if (arguments[0].get() == null) {
            return new ArrayList<String>();
        }

        String sql = arguments[0].get().toString();

        return SqlParseUtil.getColNamesFromSql(sql);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }


}
