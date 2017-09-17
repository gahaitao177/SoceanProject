import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by root on 2017/1/18.
 */
public class TimeFormatTest {

    public static void main(String[] args) {

        String[] date = {"2017-01-01 12:12:12", "2017-01-02 12:12:12", "2017-01-03 12:12:12", "2017-01-04 12:12:12",
                "2017-01-05 12:12:12", "2017-01-06 12:12:12", "2017-01-07 12:12:12", "2017-01-08 12:12:12",
                "2017-01-09 12:12:12", "2017-01-10 12:12:12", "2017-01-11 12:12:12", "2017-01-12 12:12:12",
                "2017-01-13 12:12:12", "2017-01-14 12:12:12", "2017-01-15 12:12:12", "2017-01-16 12:12:12",
                "2017-01-17 12:12:12", "2017-01-18 12:12:12", "2017-01-19 12:12:12", "2017-01-20 12:12:12",
                "2017-01-15 12:12:21", "2017-01-15 12:12:22"};

        for (int i = 0; i < date.length; i++) {
            System.out.println("今天" + date[i] + "是第几周：" + getWeek(date[i]));
        }

    }

    /**
     * 得到输入日期的年份
     *
     * @param date
     * @return
     */
    public static int getYear(String date) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            cal.setTime(format.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int year = cal.get(Calendar.YEAR);
        return year;
    }

    /**
     * 得到一天是一年中的第几周
     *
     * @param date
     * @return
     */
    public static int getWeek(String date) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            cal.setTime(format.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        return week;
    }

    /**
     * 得到一天是一年的第几个月
     *
     * @param date
     * @return
     */
    public static int getMonth(String date) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            cal.setTime(format.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int month = cal.get(Calendar.MONTH) + 1;
        return month;
    }

    /**
     * 得到一天是一年的第几个季度
     *
     * @param date
     * @return
     */
    public static int getQuarter(String date) {
        int month = getMonth(date);
        int quarter;
        switch (month) {
            case 1:
            case 2:
            case 3:
                quarter = 1;
                break;
            case 4:
            case 5:
            case 6:
                quarter = 2;
                break;
            case 7:
            case 8:
            case 9:
                quarter = 3;
                break;
            case 10:
            case 11:
            case 12:
                quarter = 4;
                break;
            default:
                quarter = 0;
                break;
        }

        return quarter;
    }

}
