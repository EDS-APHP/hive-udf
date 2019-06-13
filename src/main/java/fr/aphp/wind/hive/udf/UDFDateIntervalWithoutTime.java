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

import java.util.Date;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 * Cette classe regrouppe les fonctions récurrentes de date à utiliser dans Hive
 * 
 * 
 */

@Description(name = "geDateDiffWithoutTime", value = " The date difference without time" 
,extended = "Example:\n" +
        "    SELECT _FUNC_(dateDebNda, dateFinNda, DATE_DIFF_YEAR) FROM patient;\n" +
		"    SELECT _FUNC_(dateDebNda, dateFinNda, DATE_DIFF_DAYS) FROM patient;\n + "
+ "the paramaeter unit takes the values DATE_DIFF_DAYS, DATE_DIFF_YEAR, DATE_DIFF_MONTH") 
public class UDFDateIntervalWithoutTime extends UDF {

	private static final Logger LOG = Logger.getLogger(UDFDateIntervalWithoutTime.class);

	/**
	 * Calculate the difference between two dates as number of days
	 * 
	 * @param d8Start - Date début
	 * @param d8Inter - Date intermédiaire, si non null remplace d8End
	 * @param d8End   - Date fin
	 * @return - l'interval en nombre de jours entre d8Start et d8End
	 */

	public Integer evaluate(Timestamp d8Start, Timestamp d8End, String unit) {
		if (d8Start == null)
			return null;

		DateTime _d8End = new DateTime(d8End.toEpochMilli());
		DateTime _d8Start = new DateTime(d8Start.toEpochMilli());

		Period ageCourJour = new Period(_d8Start, _d8End);
		DateTime zero = new DateTime(0);

		Long tmp;

		switch (unit) {

		case "DATE_DIFF_DAYS":
			tmp = ageCourJour.toDurationFrom(zero).getStandardDays();
			//return ageCourJour.getDays();
			return tmp.intValue();

		case "DATE_DIFF_YEAR": {
			return ageCourJour.getYears();

		}
		case "DATE_DIFF_MONTH": {
			tmp = ageCourJour.toDurationFrom(zero).getStandardDays();
			return ageCourJour.getMonths();
		}

		default:
			return null;
		}

	}

}
