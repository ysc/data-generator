package org.apdplat.data.generator.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Created by ysc on 07/02/2017.
 */
public class TimeUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeUtils.class);

    public static String toString(long time, String format){
        if(format == null){
            return "";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        return simpleDateFormat.format(new Date(time));
    }

    public static String toString(Date time, String format){
        if(time == null || format == null){
            return "";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        return simpleDateFormat.format(time);
    }


    public static String toString(long time){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(new Date(time));
    }

    public static String toString(Date time){
        if(time == null){
            return "";
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(time);
    }


    public static Long fromString(String time){
        try {
            if (time.length() == 8 && !time.contains("/") && !time.contains("-")) {
                return fromString(time, "yyyyMMdd");
            }
            if (time.length() == 10 && time.contains("/")) {
                return fromString(time, "yyyy/MM/dd");
            }
            if (time.length() == 10 && time.contains("-")) {
                return fromString(time, "yyyy-MM-dd");
            }
            if (time.length() == 19 && time.contains("-") && time.contains(":")) {
                return fromString(time, "yyyy-MM-dd HH:mm:ss");
            }
            if (time.length() == 19 && time.contains("/") && time.contains(":")) {
                return fromString(time, "yyyy/MM/dd HH:mm:ss");
            }
            if (time.length() == 14 && !time.contains("-") && !time.contains("/") && !time.contains(":")) {
                return fromString(time, "yyyyMMddHHmmss");
            }
        }catch (Exception e){
            // do nothing
        }
        return -1l;
    }

    public static long fromString(String time, String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
            if(date != null){
                return date.getTime();
            }
        } catch (Exception e) {
            // do nothing
            //LOGGER.error("time parse error: "+time, e);
        }
        return -1;
    }

    public static String getTimeDes(Long ms) {
        //处理参数为NULL的情况
        if(ms == null || ms == 0){
            return "0毫秒";
        }
        boolean minus = false;
        if(ms < 0){
            minus = true;
            ms = -ms;
        }
        int ss = 1000;
        int mi = ss * 60;
        int hh = mi * 60;
        int dd = hh * 24;

        long day = ms / dd;
        long hour = (ms - day * dd) / hh;
        long minute = (ms - day * dd - hour * hh) / mi;
        long second = (ms - day * dd - hour * hh - minute * mi) / ss;
        long milliSecond = ms - day * dd - hour * hh - minute * mi - second * ss;

        StringBuilder str=new StringBuilder();
        if(day>0){
            str.append(day).append("天,");
        }
        if(hour>0){
            str.append(hour).append("小时,");
        }
        if(minute>0){
            str.append(minute).append("分钟,");
        }
        if(second>0){
            str.append(second).append("秒,");
        }
        if(milliSecond>0){
            str.append(milliSecond).append("毫秒,");
        }
        if(str.length()>0){
            str.setLength(str.length() - 1);
        }

        if(minus){
            return "-"+str.toString();
        }

        return str.toString();
    }


    public static String toString(LocalDateTime time){
        return toString(time, "yyyy-MM-dd HH:mm:ss");
    }

    public static String toString(LocalDateTime time, String pattern){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return time.format(formatter);
    }

    public static LocalDateTime parse(String time){
        return parse(time, null);
    }

    public static LocalDateTime parse(String text, String pattern){
        return LocalDateTime.ofInstant(new Date(pattern==null ? fromString(text) : fromString(text, pattern)).toInstant(), ZoneId.systemDefault());
    }
    /**
     * 将时间裁剪到分钟
     * @param time
     * @return
     */
    public static long trimToMinute(long time){
        return fromString(toString(time, "yyyy-MM-dd HH:mm"), "yyyy-MM-dd HH:mm");
    }

    public static long trimToDay(long time){
        return fromString(toString(time, "yyyy-MM-dd"), "yyyy-MM-dd");
    }

    public static void main(String[] args) {
        //String time = "2015-08-06 00:00:38";
        //String time = "2015-08-16 00:00:04,199";
        String time = "2015-08-16 00:00:00";
        System.out.println("time:"+time);
        long t = fromString(time);
        System.out.println("time:"+t);
        String ts = toString(t);
        System.out.println("time:"+ts);

        System.out.println(TimeUtils.toString(new Date(), "yyyyMMddHHmm"));

        System.out.println(getTimeDes(10000l));
    }
}
