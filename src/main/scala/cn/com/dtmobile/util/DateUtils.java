//package cn.com.dtmobile.util;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import java.sql.Timestamp;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//
///**
// * 日期工具类
// *
// * @author luoyue@cn.ibm.com
// *
// */
//public class DateUtils {
//
//    private static final Log logger = LogFactory.getLog(DateUtils.class);
//
//    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
//    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
//    public static final String DATE_FORMAT = "yyyy-MM-dd";
//    public static final String TIME_FORMAT = "HH:mm:ss";
//
//    public static final String DAYTIME_START = "00:00:00";
//    public static final String DAYTIME_END = "23:59:59";
//
//    private DateUtils() {
//    }
//
//    private static final String[] FORMATS = { "yyyy-MM-dd", "yyyy-MM-dd HH:mm",
//            "yyyy-MM-dd HH:mm:ss", "HH:mm", "HH:mm:ss", "yyyy-MM" ,"yyyy-MM-dd HH:mm:ss.S"};
//
//    public static Date convert(String str) {
//        if (str != null && str.length() > 0) {
//            if (str.length() > 10 && str.charAt(10) == 'T')
//                str = str.replace('T', ' '); // 去掉json-lib加的T字母
//            for (String format : FORMATS) {
//                if (str.length() == format.length()) {
//                    try {
//                        Date date = new SimpleDateFormat(format).parse(str);
//
//                        return date;
//                    } catch (ParseException e) {
//                        if (logger.isWarnEnabled()) {
//                            logger.warn(e.getMessage());
//                        }
//                        // logger.warn(e.getMessage());
//                    }
//                }
//            }
//        }
//        return null;
//    }
//
//    public static Date convert(String str, String format) {
//        if (!StringUtils.isEmpty(str)) {
//            try {
//                Date date = new SimpleDateFormat(format).parse(str);
//                return date;
//            } catch (ParseException e) {
//                if (logger.isWarnEnabled()) {
//                    logger.warn(e.getMessage());
//                }
//                // logger.warn(e.getMessage());
//            }
//        }
//        return null;
//    }
//
//    public static String convert(Date date) {
//        return convert(date, DATE_TIME_FORMAT);
//    }
//
//    public static String convert(Date date, String dateFormat) {
//        if (date == null)
//            return null;
//
//        if (null == dateFormat)
//            dateFormat = DATE_TIME_FORMAT;
//
//        return new SimpleDateFormat(dateFormat).format(date);
//    }
//
//    /**
//     * 返回该天从00:00:00开始的日期
//     *
//     * @param date
//     * @return
//     */
//    public static Date getStartDatetime(Date date) {
//        String thisdate = convert(date, DATE_FORMAT);
//        return convert(thisdate + " " + DAYTIME_START);
//
//    }
//
//    /**
//     * 返回n天后从00:00:00开始的日期
//     *
//     * @param date
//     * @return
//     */
//    public static Date getStartDatetime(Date date, Integer diffDays) {
//        SimpleDateFormat df=new SimpleDateFormat(DATE_FORMAT);
//        String thisdate = df.format(date.getTime() + diffDays * 24 * 60 * 60 * 1000l);
//        return convert(thisdate + " " + DAYTIME_START);
//    }
//
//    /**
//     * 返回该天到23:59:59结束的日期
//     *
//     * @param date
//     * @return
//     */
//    public static Date getEndDatetime(Date date) {
//        String thisdate = convert(date, DATE_FORMAT);
//        return convert(thisdate + " " + DAYTIME_END);
//
//    }
//
//    /**
//     * 返回n天到23:59:59结束的日期
//     *
//     * @param date
//     * @return
//     */
//    public static Date getEndDatetime(Date date, Integer diffDays) {
//        SimpleDateFormat df=new SimpleDateFormat(DATE_FORMAT);
//        String thisdate = df.format(date.getTime() + diffDays * 24 * 60 * 60 * 1000l);
//        return convert(thisdate + " " + DAYTIME_END);
//
//    }
//
//    /**
//     * 返回该日期的最后一刻，精确到纳秒
//     *
//     * @param date
//     * @return
//     */
//    public static Timestamp getLastEndDatetime(Date endTime) {
//        Timestamp ts = new Timestamp(endTime.getTime());
//        ts.setNanos(999999999);
//        return ts;
//    }
//    /**
//     * 返回该日期加1秒
//     *
//     * @param date
//     * @return
//     */
//    @SuppressWarnings("static-access")
//    public static Timestamp getEndTimeAdd(Date endTime){
//        Timestamp ts = new Timestamp(endTime.getTime());
//        Calendar c = Calendar.getInstance();
//        c.setTime(ts);
//        c.add(Calendar.MILLISECOND, 1000);
//        c.set(c.MILLISECOND, 0);
//        return new Timestamp(c.getTimeInMillis());
//    }
//    /**
//     * 相对当前日期，增加或减少天数
//     * @param date
//     * @param day
//     * @return
//     */
//    public static String addDay(Date date, int day){
//        SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
//        return df.format(new Date(date.getTime() + day * 24 * 60 * 60 * 1000));
//    }
//
//
//    /**
//     * 相对当前日期，增加或减少天数
//     * @param date
//     * @param day
//     * @return
//     */
//    public static String addDay(String date, int day,String format){
//        SimpleDateFormat df = new SimpleDateFormat(format);
//        return df.format(new Date(DateUtils.convert(date,format).getTime() + day * 24 * 60 * 60 * 1000));
//    }
//
//
//    /**
//     * 返回两个时间的相差天数
//     * @param startTime 对比的开始时间
//     * @param endTime 对比的结束时间
//     * @return 相差天数
//     */
//
//    public static Long getTimeDiff(String startTime, String endTime) {
//        Long days = null;
//        Date startDate=null;
//        Date endDate=null;
//        try {
//            if(startTime.length()==10 && endTime.length()==10){
//                startDate = new SimpleDateFormat(DATE_FORMAT).parse(startTime);
//                endDate = new SimpleDateFormat(DATE_FORMAT).parse(endTime);
//            }else{
//                startDate = new SimpleDateFormat(DATE_TIME_FORMAT).parse(startTime);
//                endDate = new SimpleDateFormat(DATE_TIME_FORMAT).parse(endTime);
//            }
//
//            Calendar c = Calendar.getInstance();
//            c.setTime(startDate);
//            long l_s = c.getTimeInMillis();
//            c.setTime(endDate);
//            long l_e = c.getTimeInMillis();
//            days = (l_e - l_s) / 86400000;
//        } catch (ParseException e) {
//            if (logger.isWarnEnabled()) {
//                logger.warn(e.getMessage());
//            }
//            days = null;
//        }
//        return days;
//    }
//
//    public static String getPidFromDate(Date date) {
//        if (date == null)
//            return "";
//
//        String m = convert(date, "yyyyMM");
//        String d = convert(date, "dd");
//
//        if (Integer.valueOf(d) <= 10)
//            d = "01";
//        else if (Integer.valueOf(d) <= 20)
//            d = "02";
//        else
//            d = "03";
//
//        return m.concat(d);
//    }
//
//
//    public static void main(String[] args) throws Exception {
////    	Date date = convert("2014-10-14 12:13:23.542000", "yyyy-MM-dd HH:mm:ss.SSSSSS");
//        Date date = convert("2014-10-14 12:13:23.432", "yyyy-MM-dd HH:mm:ss.SSS");
///*    	long a = date.getTime();
//    	String str = convert(new Date(a), "yyyy-MM-dd HH:mm:ss.SSS");
////    	System.out.println(date);
//    	System.out.println(str);
//    	String b = "2014-10-14 12:13:23.431000";
//    	System.out.println(b.substring(0,23));
//    	System.out.println("a,b,c".split(",").length);*/
//
//        System.out.println(DateUtils.addDay("20170401",-1,"yyyyMMdd"));
////
////        Date date = new Date();
////        System.out.println(getEndDatetime(date));
////        System.out.println(getStartDatetime(date,-3));
////        System.out.println(getEndDatetime(date,3));
////        System.out.println(addDay(new Date(), 1));
////        System.out.println(addDayNew(new Date(), Long.parseLong("-28")));
////        System.out.println(addDay(new Date(),0));
//    }
//}
