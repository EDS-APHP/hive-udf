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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

@Description(name = "formatDate_dd/MM/YYYY", value = " Modifie le format de date sans l'heure"
,extended = "Example:\n" +
        "    SELECT _FUNC_(dateDebNda) FROM patient;\n" )

public class UDFFormatDate extends UDF {

	private static final Logger LOG = Logger.getLogger(UDFFormatDate.class);

	/**
	 * This method formats a date t remove time by setting time to zero
	 * 
	 * @param dt - the date to format
	 * @return the date without time in the format dd/MM/yyyy
	 */

	public Date evaluate(Date dt) {
		if (dt == null)
			return null;
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		try {
			Date todayWithZeroTime = formatter.parse(formatter.format(dt));
			return todayWithZeroTime;
		} catch (ParseException e) {
		}
		return null;
	}

}
