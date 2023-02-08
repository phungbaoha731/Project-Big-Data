package bk.edu.utils;


import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeUtil {

    public static String getDate(String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(System.currentTimeMillis() - 86400000);
    }


}
