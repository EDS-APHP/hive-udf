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

public class UDFDateFunctions extends UDF {

	private static final Logger LOG = Logger.getLogger(UDFDateFunctions.class);

	
	/**
	 * This method calculates the difference between 2 dates
	 * 
	 * @param d8Start - start date 
	 * @param d8End - end date
	 * @param unit - the unit parameter "Y":to calculate year and "D" to calculate days including the time  
	 * @return - difference between 2 dates according to the unit parameter 
	 */	
	@Description(name = "getDateDiff", value = " The date difference as years or days")
	public Double evaluate(Date d8Start, Date d8End, String unit) {

		if (d8Start == null || d8End == null) {
			return null;
		}
		// get difference between two dates as years including the months as a fraction of
		// year
		if (unit.equals("Y")) {

			DateTime _d8End = new DateTime(d8End);
			DateTime _d8Start = new DateTime(d8Start);
			Period period;
			period = new Period(_d8Start, _d8End);
			int years = period.getYears();
			int month = period.getMonths();
			return Precision.round((Double) (years + (Double) (month / 12.0)), 1);
		} else {
			// get difference between two dates as day units including minutes and seconds
			if (unit.equals("D")) {
				try {
					DateTime d8EndP = new DateTime(d8End);
					DateTime d8StartP = new DateTime(d8Start);

					Duration duration = new Duration(d8StartP, d8EndP);
					return Precision.round(((Long) duration.getStandardSeconds()).doubleValue() / (60 * 60 * 24), 2);

				} catch (Exception e) {
					e.printStackTrace();
					return null;
				}

			} else {
				return null;
			}
		}

	}

	
	/**
	 * Calculate the difference between two dates as number of days
	 * 
	 * @param d8Start - Date début
	 * @param d8Inter - Date intermédiaire, si non null remplace d8End
	 * @param d8End   - Date fin
	 * @return - l'interval en nombre de jours entre d8Start et d8End
	 */
	@Description(name = "getDateDiffAsDaysWithoutTime", value = " The date difference as number of days")
	public Integer evaluate(Date d8Start, Date d8End) {
		if (d8Start == null)
			return null;

		DateTime _d8End = new DateTime(d8End);
		DateTime _d8Start = new DateTime(d8Start);

		Period ageCourJour = new Period(_d8Start, _d8End);

		DateTime zero = new DateTime(0);
		Long tmp = ageCourJour.toDurationFrom(zero).getStandardDays();
		return tmp.intValue();
	}

	
	/**
	 * This method formats a date t remove time by setting time to zero
	 * @param dt - the date to format
	 * @return the date without time in the format dd/MM/yyyy
	 */
	@Description(name = "getDateFromDateTime", value = " Modifie le format de date sans l'heure")
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

	
	/**
	 * 
	 * @param dt - the date parameter
	 * @param datePart - the part of the date to extract 
	 * @return - date part year, month,day,hour,minute,second 
	 */
	@Description(name = "gePartOfDateTime", value = " Returns a part of the date depending on the datePart parameter")
	public Integer evaluate(Date dt, DateEnum datePart) {

		if (dt == null || datePart == null)
			return null;

		DateTime _dt = new DateTime(dt);

		switch (datePart) {

		case HOUR_PART:
			return _dt.getHourOfDay();
		case MINUTES_PART:
			return _dt.getMinuteOfDay();
		case SECONDS_PART:
			return _dt.getSecondOfDay();
		case YEAR_PART:
			return _dt.getYear();
		case DATE_PART:
			return null;
		case DAY_PART:
			return _dt.getDayOfMonth();
		case MONTH_PART:
			return _dt.getMonthOfYear();
		case TIME_PART:
			return null;
		default:
			return null;
		}

	}

	
	@Deprecated
	@Description(name = "getDateDiffAsYears", value = " The date difference as number of years")

	/**
	 * Cette méthode calcule la diiférence entre deux dates
	 * 
	 * @param d8Start - Date début
	 * @param d8End   - Date fin
	 * @return - l'interval en années entre entre d8Start et d8End en prenant compte
	 *         les mois
	 */
	public Double getDateDiffAsYears(Date d8Start, Date d8End) {

		if (d8Start == null || d8End == null) {
			return null;
		}
		DateTime _d8End = new DateTime(d8End);
		DateTime _d8Start = new DateTime(d8Start);
		Period period;
		period = new Period(_d8Start, _d8End);
		int years = period.getYears();
		int month = period.getMonths();
		return Precision.round((Double) (years + (Double) (month / 12.0)), 1);
	}

	//@Description(name = "getDateDiffAsDaysFraction", value = " The date difference as minutes")
	@Deprecated
	/**
	 * Calculate difference between two dates in minutes
	 * 
	 * @param d8Start - Date début
	 * @param d8End   - Date fin
	 * @return l'interval en nombre de minutes entre d8Start et d8End
	 */

	public Double duree(Date d8Start, Date d8End) {

		if (d8Start == null || d8End == null) {
			return null;
		}
		try {
			DateTime d8EndP = new DateTime(d8End);
			DateTime d8StartP = new DateTime(d8Start);

			Duration duration = new Duration(d8StartP, d8EndP);
			return Precision.round(((Long) duration.getStandardSeconds()).doubleValue() / (60 * 60 * 24), 2);

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
