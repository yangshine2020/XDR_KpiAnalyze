package mr;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {

	public static final String TIMESTAMP_FORMAT = "yyyyMMddHH";
	public static final String TIMESTAMP_FORMAT_S = "yyyy-MM-dd HH:mm:ss.S";
	
	public static String getDateTime(String time) {		
		Long ts = Long.parseLong(time) ;
		SimpleDateFormat dfs = new SimpleDateFormat(TIMESTAMP_FORMAT);
		return dfs.format(new Date(ts));
	}
	
	
	public static String stringToDate(String date){
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH") ;
		SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S") ;		
		String format = null ;
		try {
			Date parse = simpleDateFormat2.parse(date) ;
			 format = simpleDateFormat.format(parse) ;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return format;	
	}
	
}
