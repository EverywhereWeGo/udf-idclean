package com.bfd;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

public class IdClean extends UDF {
	public String evaluate(String idstring) {
		if (checkLength(idstring) && checkLastcode(idstring) && checkProvice(idstring)&& checkBirthday(idstring)) {
			return "true";
		}
		return "false";
	}

	public Boolean checkLength(String idstring) {
		if (idstring.length() == 18) {
			return true;
		}
		return false;
	}

	public Boolean checkLastcode(String idstring) {
		char idchar[] = idstring.toCharArray();
		int num = 0;
		int weight[] = new int[] { 7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2 };
		int idint[] = new int[17];
		for (int i = 0; i < idchar.length - 1; i++) {
			idint[i] = idchar[i] - '0';
			if (idint[i] < 0 || idint[i] > 9) {
				return false;
			}
			num = num + idint[i] * weight[i];
		}
		char mods[] = new char[] { '1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2' };
		if (mods[num % 11] == idchar[17]) {
			return true;
		}
		return false;
	}

	public Boolean checkBirthday(String idstring) {
		String birth = idstring.substring(6, 14);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		sdf.setLenient(false);
		try {
			Date birthday = sdf.parse(birth);
			Calendar date = Calendar.getInstance();
			date.setTime(birthday);
			Calendar begin = Calendar.getInstance();
			begin.setTime(new SimpleDateFormat("yyyyMMdd").parse("190001001"));
			Calendar end = Calendar.getInstance();
			end.setTime(new SimpleDateFormat("yyyyMMdd").parse("20250101"));
			if (date.after(begin) && date.before(end)) {
				return true;
			}
		} catch (ParseException e) {
			return false;
		}
		return false;
	}

	public Boolean checkProvice(String idstring) {
		char idchar[] = idstring.toCharArray();
		int provice = (idchar[0] - '0') * 10 + (idchar[1] - '0');
		int[] provices = new int[] { 11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 42, 43, 44, 45, 46,
				50, 51, 52, 53, 54, 61, 62, 63, 64, 65, 71, 81, 82 };
		int res = Arrays.binarySearch(provices, provice);
		if (res >= 0) {
			return true;
		}
		return false;
	}
}