package fr.aphp.wind.hive.udf;

import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "string_subsequence_keep", value = "array<string> _FUNC_(string, pattern) - split a string according to pattern and keep that pattern")
public class UDFsplitKeep extends UDF {

	private String lastRegex = null;
	private Pattern p = null;

	public LinkedList<String> evaluate(String s, String regex) {
		if (s == null || regex == null) {
			return null;
		}
		if (!regex.equals(lastRegex)) {
			lastRegex = regex;
			p = Pattern.compile(regex);
		}
		Matcher m = p.matcher(s);
		LinkedList<String> ret = new LinkedList<String>();
		int previousBegin = 0;
		while (m.find()) {
			ret.add(s.substring(previousBegin, m.start()));
			ret.add(m.group(0));
			previousBegin = m.end();
		}
		if (previousBegin != s.length()) {
			ret.add(s.substring(previousBegin, s.length()));
		}

		return ret;
	}
}