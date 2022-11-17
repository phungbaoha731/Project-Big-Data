package bk.edu.utils;


import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeUtil {

    public static String getDate(String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return sdf.format(cal.getTime());
    }


}
