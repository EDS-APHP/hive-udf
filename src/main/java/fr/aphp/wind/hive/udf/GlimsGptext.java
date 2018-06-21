package fr.aphp.wind.hive.udf;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class GlimsGptext {
  private HashMap<String, String> map;
  private Pattern typeGptextPattern;

  public GlimsGptext(String path) {
    map = new HashMap<String, String>();
    generateMap(path);
    typeGptextPattern = Pattern.compile("\\{<[\\+]?([\\w_]+?)\\}");
  }

  private void generateMap(String path) {

    CsvParserSettings parserSettings = new CsvParserSettings();
    // the file used in the example uses '\n' as the line separator sequence.
    // the line separator sequence is defined here to ensure systems such as MacOS and Windows
    // are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
    parserSettings.getFormat().setLineSeparator("\n");
    parserSettings.setHeaderExtractionEnabled(false);
    parserSettings.setQuoteDetectionEnabled(true);
    parserSettings.getFormat().setCharToEscapeQuoteEscaping('"');
    parserSettings.getFormat().setDelimiter(",".charAt(0));
    parserSettings.setMaxCharsPerColumn(-1);

    // creates a CSV parser
    CsvParser parser = new CsvParser(parserSettings);
    parser.beginParsing(getReader(path));

    String[] row;
    while ((row = parser.parseNext()) != null) {
      map.put(row[0].toLowerCase(), row[1]);
    }

    // The resources are closed automatically when the end of the input is reached,
    // or when an error happens, but you can call stopParsing() at any time.

    // You only need to use this if you are not parsing the entire content.
    // But it doesn't hurt if you call it anyway.
    parser.stopParsing();
  }

  public Reader getReader(String relativePath) {
    try {
      return new FileReader(relativePath);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public String populateGptext(String s, String source) {
    if (s == null) {
      return null;
    }

    Matcher m = this.typeGptextPattern.matcher(s);
    LinkedList<String> ret = new LinkedList<String>();
    int previousBegin = 0;
    while (m.find()) {
      ret.add(s.substring(previousBegin, m.start()));
      ret.add(fetchGptext(m, source));
      previousBegin = m.end();
    }
    if (previousBegin != s.length()) {
      ret.add(s.substring(previousBegin, s.length()));
    }
    if (previousBegin == 0)
      return ret.get(0);

    return populateGptext(String.join("", ret), source);
  }

  private String fetchGptext(Matcher match, String source) {
    String value = source + match.group(1).toLowerCase();
    String key = map.get(value);
    return key;
  }

  public Pattern getTypeGptextPattern() {
    return this.typeGptextPattern;

  }


}
