package fr.aphp.wind.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "string_quote", value = "string _FUNC_(string) - Return a quoted string for postgresql COPY")
public class UDFquote extends UDF {

    public String evaluate(String s, String sep) {
	if (s == null) {
	    return null;
	}
	if(s.indexOf(sep) == -1 && s.indexOf("\"") == -1 && s.indexOf("\n") == -1) {
	    return s;
	}
	
	return prepareString(s);
    }

    public String prepareString(String str) {
	if (str.equals("")) {
	    return "";
	} else {
	    return "\"" + str.replaceAll("\"", "\"\"") // for quoting
		    .replaceAll("(?m)^\\\\.$", "") // because of postgresql
						   // parser
		    + "\"";
	}
    }
}
