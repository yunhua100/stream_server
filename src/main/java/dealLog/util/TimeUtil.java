package dealLog.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TimeUtil {
    public static String getNowTime(){
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        Calendar ca = Calendar.getInstance();

        return sf.format(ca.getTime());
    }
    public static String getTimeByCurr(long interval){
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar ca = Calendar.getInstance();
        ca.setTimeInMillis(interval);
        return sf.format(ca.getTime());
    }

    public static String getTime(){
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar ca = Calendar.getInstance();

        return sf.format(ca.getTime());
    }

    /**
     * 将传递的时间字符串转为毫秒值
     * @param timeStr 字符串(yyyy-MM-dd HH:mm:ss)
     * @return 毫秒值
     * @throws ParseException
     */
    public static Long getMillTime(String timeStr) throws ParseException{
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar ca = Calendar.getInstance();
        ca.setTime(sf.parse(timeStr));

        return ca.getTimeInMillis();
    }

    /**
     * 将传递的时间字符串转为毫秒值
     * @param timeStr 字符串(yyyy-MM-dd HH:mm:ss)
     * @return 毫秒值
     * @throws ParseException
     */
    public static Long changeToMillTime(String timeStr){
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar ca = Calendar.getInstance();
        try {
            ca.setTime(sf.parse(timeStr));
            return ca.getTimeInMillis();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return 0l;
    }

    /**
     * 将毫秒值转为yyyy-MM-dd HH:mm:ss
     * @param millTime
     * @return
     * @throws ParseException
     */
    public static String getFormatTime(String millTime)throws ParseException{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(millTime));

        return simpleDateFormat.format(calendar.getTime());
    }

    public static String test(Long millTime)throws ParseException{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millTime);

        return simpleDateFormat.format(calendar.getTime());
    }

    public static Date getParseTime(String dataStr)throws ParseException{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return simpleDateFormat.parse(dataStr);
    }


    /**
     * 导入excel时，格式化时间
     * @param date
     * @return
     */
    public static String parseExcelDate(Date date){
        SimpleDateFormat sfExcel = new SimpleDateFormat("yyyy年MM月dd日");
        return sfExcel.format(date);
    }

    /**
     * 获取当前时间
     * @return
     */
    public static String getCurrTime(){
        SimpleDateFormat sfTime = new SimpleDateFormat("yyyy-MM-dd");
        Calendar ca = Calendar.getInstance();

        return sfTime.format(ca.getTime());
    }

    /**
     * 获取当前时间
     * @return
     */
    public static Date getCurrDate(){
        Calendar ca = Calendar.getInstance();

        return ca.getTime();
    }

    /**
     * 日期格式字符串转换成时间戳
     * @param date_str 字符串日期
     * @param format 如：yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static String date2TimeStamp(String date_str,String format){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(date_str).getTime()/1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 时间戳格式化成标准格式 yyyy-MM-dd'T'HH:mm:ss.SSS.'Z'
     * @param date_str 字符串日期
     * @param format 如：yyyy/MM/dd HH:mm:ss
     */
    public static String timeStampFormat(String date_str,String format){
        try {
            SimpleDateFormat parseSdf = new SimpleDateFormat(format);
            SimpleDateFormat formatSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            return formatSdf.format(parseSdf.parse(date_str));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date_str;
    }

    /**
     * 获取当前时间戳
     * @return yyyy-MM-dd'T'HH:mm:ss.SSS.'Z'
     */
    public static String getCurrentTimestamp(){
        SimpleDateFormat sfTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Calendar ca = Calendar.getInstance();
        return sfTime.format(ca.getTime());
    }



    /**
     * 获取当前时间的毫秒值
     * @return
     */
    public static long getMillTimeAtNow(){
        Calendar calendar = Calendar.getInstance();

        return calendar.getTimeInMillis();
    }

    /**
     * 将Influx中的时间转为yyyy-MM-dd HH:mm:ss
     * @param influxTime
     * @return
     */
    public static String formatInfluxTime(String influxTime){
        influxTime = influxTime.replace("T", " ").replace("Z", "").split("\\.")[0];

        return influxTime;
    }

    /**
     * 获取时间差值（分钟）
     * @param beginTime
     * @param endTime
     * @return
     */
    public static long getDiffMinTime(String beginTime,String endTime){
        long bTime = Long.parseLong(beginTime);
        long eTime = Long.parseLong(endTime);

        long diffTime = (eTime - bTime)/(1000 * 60);

        return diffTime;
    }

    /**
     * 获取时间差值（小时）
     * @param beginTime
     * @param endTime
     * @return
     */
    public static long getDiffHourTime(String beginTime,String endTime){
        long bTime = Long.parseLong(beginTime);
        long eTime = Long.parseLong(endTime);

        long diffTime = (eTime - bTime)/(1000 * 60 * 60);

        return diffTime;
    }

    public static String dayFormat(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(date);
    }

    /**
     * 计算两个日期之间相差的天数
     * @param smdate 较小的时间
     * @param bdate  较大的时间
     * @return 相差天数
     * @throws ParseException
     */
    public static int daysBetween(Date smdate,Date bdate) throws ParseException
    {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
        smdate=sdf.parse(sdf.format(smdate));
        bdate=sdf.parse(sdf.format(bdate));
        Calendar cal = Calendar.getInstance();
        cal.setTime(smdate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(bdate);
        long time2 = cal.getTimeInMillis();
        long between_days=(time2-time1)/(1000*3600*24);

        return Integer.parseInt(String.valueOf(between_days));
    }

    /**
     * 时间戳转换成日期格式字符串
     *
     * @param seconds
     *            精确到秒的字符串
     * @param format
     * @return
     */
    public static String timeStamp2Date(String seconds, String format) {
        if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds)));
    }

    /**
     * 毫秒值转成UTC时间
     * @param millTime
     * @return
     */
    public static String millTimeToUtc(String millTime){
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(millTime));

        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return utcFormat.format(calendar.getTime());
    }

    /**
     * utc时间转化为毫秒
     * @param utcTimeStr
     * @return
     */
    public static long utcToMilltime(String utcTimeStr){
        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date utcDate = null;
        try {
            utcDate = utcFormat.parse(utcTimeStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(utcDate);
        return calendar.getTimeInMillis();

    }

    /**
     * 将毫秒值转为yyyy-MM-dd HH:mm:ss
     * @param millTime
     * @return
     * @throws ParseException
     */
    public static String getFormatTimeForLog(String millTime)throws ParseException{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(millTime));

        return simpleDateFormat.format(calendar.getTime());
    }


    public static Integer getWeekNow(){
        Calendar calendar = Calendar.getInstance();

        return calendar.get(Calendar.DAY_OF_WEEK);
    }

    public static Integer getHourNow(){
        Calendar calendar = Calendar.getInstance();

        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static void main(String[] args) {
        System.out.println(getCurrentTimestamp());
    }
}
