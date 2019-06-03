package fr.aphp.wind.hive.udf;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

@Description(name = "regexp_extract_all", value = "array<string> _FUNC_(array<String>, String) - split a string according to patternn in the array")
public class UDFSplitKeepList extends UDF {

	private ArrayList<Pattern> listRegexPattern;
	private String content;

	private void initialize(ArrayList<String> listRegex, String texteToScan) throws UDFArgumentException {

		this.listRegexPattern = new ArrayList<Pattern>();
		this.content = texteToScan;

		this.listRegexPattern = new ArrayList<Pattern>();
		this.content = texteToScan;

		for (String regex : listRegex) {
			if (!isValidRegexGroup(regex)) {
				throw new UDFArgumentException("Missing brackets, regex is not a group:" + regex);
			} else {
				Pattern p = null;
				try {
					p = Pattern.compile(regex);
					listRegexPattern.add(p);
					System.out.println("regex compile:" + regex);
				} catch (PatternSyntaxException e) {
					e.printStackTrace();
					throw new UDFArgumentException("Regex pattern syntax error:" + regex);

				}

			}
		}
	}

	public LinkedList<String> evaluate(ArrayList<String> listRegex, String texteToScan) throws UDFArgumentException {

		initialize(listRegex, texteToScan);

		LinkedList<String> ret = new LinkedList<String>();

		if (listRegexPattern == null || content == null) {

			return null;
		}

		System.out.println("before loop");
		// Loop on the regular expression list
		for (Pattern p : listRegexPattern) {
		
			System.out.println("in the loop");
			// matcher of the regular expression
			Matcher m = p.matcher(content);

			System.out.println("la regex en cours:" + p.toString());
			System.out.println("le nombre de groupes:" + m.groupCount());
			
			if (m.groupCount() > 1) {
				 throw new UDFArgumentException("Multiple groups of regex not supported:" + p.pattern());
		 	}

			int previousBegin = 0;

			// loop to find the text matching the expression
			while (m.find()) {

				System.out.println("start position:" + m.start() + ": end position" + m.end());
				System.out.println("split:" + content.substring(previousBegin, m.start()) + ":previous begin"
						+ previousBegin + ":");
				ret.add(content.substring(previousBegin, m.start()).trim());
				ret.add(m.group(1).trim());
				previousBegin = m.end();
			}

			if (previousBegin != content.length()) {
				ret.add(content.substring(previousBegin, content.length()).trim());
			}

		}
	

		return ret;
	}

	/**
	 * This methods checks if the regular expression respects the syntax of a group, it checks for
	 * the left and the right brackets
	 * 
	 * @param regexIn the regular expression to check
	 * @return true/false
	 */
	public boolean isValidRegexGroup(String regexIn) {

		String regex = "^\\(.*\\)$";

		Pattern regexP = Pattern.compile(regex);

		Matcher m = regexP.matcher(regexIn);

		return m.find();

	}

}