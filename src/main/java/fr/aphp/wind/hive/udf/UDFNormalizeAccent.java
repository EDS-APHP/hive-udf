package fr.aphp.wind.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.Normalizer;
import java.util.regex.Pattern;

@Description(name = "normalize_accent", value = "string _FUNC_(string) - Return a string by replacing all accents with its normalize values")
public class UDFNormalizeAccent extends UDF{

    public String evaluate(String str) {
        if (str == null) {
            return null;
        }

        return normalizeString(str);
    }

    private String normalizeString(String str) {
        if( str.length()==0){
            return str;
        } else {
            String strTemp = Normalizer.normalize(str, Normalizer.Form.NFD);
            Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
            return pattern.matcher(strTemp).replaceAll("");
        }
    }
}