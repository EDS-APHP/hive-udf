package fr.aphp.wind.hive.udf;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import java.sql.Timestamp;
import java.sql.Date;
import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 * Cette classe regrouppe les fonctions récurrentes de date à utiliser dans Hive
 * 
 * 
 */


@Description(name = "getDateDiff", value = " value = \"Double _FUNC_(Date d8Start, Date d8End, DateEnum unit)-calculates the difference between two dates, including the seconds" 
,extended = "Example:\n" +
        "    SELECT _FUNC_(dateDebNda, dateFinNda, DateEnum.DATE_DIFF_YEAR) FROM patient;\n" +
		
"    SELECT _FUNC_(dateDebNda, dateFinNda, DateEnum.DATE_DIFF_DAYS) FROM patient;\n") 

public class UDFDateInterval extends UDF {

	private static final Logger LOG = Logger.getLogger(UDFDateInterval.class);

	/**
	 * This method calculates the difference between 2 dates
	 * 
	 * @param d8Start - start date
	 * @param d8End   - end date
	 * @param unit    - the unit parameter "DATE_DIFF_YEAR":to calculate year and "DATE_DIFF_DAYS" to
	 *                calculate days including the time
	 * @return - difference between 2 dates according to the unit parameter
	 */
		public Date evaluate(Timestamp d8Start, Timestamp d8End, String unit) {

		if (d8Start == null || d8End == null) {
			return null;
		}
		return new Date(d8Start.getTime());
		// get difference between two dates as years including the months as a fraction
		// of
		// year

		// switch (unit) {

		// case "DATE_DIFF_YEAR": {
		// 	DateTime _d8End = new DateTime(d8End.getTime());
		// 	DateTime _d8Start = new DateTime(d8Start.getTime());
		// 	Period period;
		// 	period = new Period(_d8Start, _d8End);
		// 	int years = period.getYears();
		// 	int month = period.getMonths();
		// 	return Precision.round((Double) (years + (Double) (month / 12.0)), 1);
		// }
		//
		// case "DATE_DIFF_DAYS": {
		// 	try {
		// 		DateTime d8EndP = new DateTime(d8End.getTime());
		// 		DateTime d8StartP = new DateTime(d8Start.getTime());

		// 		Duration duration = new Duration(d8StartP, d8EndP);
		// 		return Precision.round(((Long) duration.getStandardSeconds()).doubleValue() / (24*60*60), 2);

		// 	} catch (Exception e) {
		// 		e.printStackTrace();
		// 		return null;
		// 	}
		// }

		// default:
		// 	return null;
		//}

	}


}
