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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

/**
 * Cette classe regrouppe les fonctions récurrentes de date à utiliser dans Hive
 * 
 * 
 */
@Description(name = "extractDatePart", value = " Returns a part of the date depending on the datePart parameter"
,extended = "Example:\n" +
        "    SELECT _FUNC_(dateDebNda, DateEnum.HOUR_ONLY) FROM patient;\n" +
		
"    SELECT _FUNC_(dateDebNda, DateEnum.MINUTES_ONLY) FROM patient;\n") 
public class UDFExtractDatePart extends UDF {

	private static final Logger LOG = Logger.getLogger(UDFExtractDatePart.class);

	
	/**
	 * 
	 * @param dt - the date parameter
	 * @param datePart - the part of the date to extract 
	 * @return - date part year, month,day,hour,minute,second 
	 */
	
	public Integer evaluate(Date dt, DateEnum datePart) {

		if (dt == null || datePart == null)
			return null;

		DateTime _dt = new DateTime(dt);

		switch (datePart) {

		case HOUR_ONLY:
			return _dt.getHourOfDay();
		case MINUTES_ONLY:
			return _dt.getMinuteOfDay();
		case SECONDS_ONLY:
			return _dt.getSecondOfDay();
		case YEAR_ONLY:
			return _dt.getYear();
		case DATE_ONLY:
			return null;
		case DAY_ONLY:
			return _dt.getDayOfMonth();
		case MONTH_ONLY:
			return _dt.getMonthOfYear();
		case TIME_ONLY:
			return null;
		default:
			return null;
		}

	}



}
