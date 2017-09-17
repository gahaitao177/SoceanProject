import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by root on 2016/12/27.
 */
public class SystemTimeTest {

    public static void main(String[] args) throws ParseException {

        /*SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(df.format(new Date()));*/

        /* String s = "2011-07-09 12:22:33";
        Date date2 = dateFormat.parse(s);
        System.out.println("-----------" + date2);*/

        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String nowDate = dateFormat.format(now);
        nowDate = "2017-01-01 12:22:33";
        System.out.println("**********" + nowDate);

        Calendar c = Calendar.getInstance();

        String year = String.valueOf(c.get(Calendar.YEAR));
        String month = String.valueOf(c.get(Calendar.MONTH));
        String week = String.valueOf(c.get(Calendar.WEEK_OF_YEAR));
        String date = String.valueOf(c.get(Calendar.DATE));
        String hour = String.valueOf(c.get(Calendar.HOUR_OF_DAY));
        String minute = String.valueOf(c.get(Calendar.MINUTE));
        String second = String.valueOf(c.get(Calendar.SECOND));

        int aa = c.get(Calendar.MONTH);

        System.out.println(year + "/" + c.get(Calendar.MONTH) + "/" + week + "/" + date + "/" + hour + "/" +
                minute + "/" + second);

        String day = year + "-" + month + "-" + date;

        String whereCondition = "time >= '" + day + " 00:00:00' and time <= '" + day + " 23:59:59'";

        System.out.println(whereCondition);


    }
}
