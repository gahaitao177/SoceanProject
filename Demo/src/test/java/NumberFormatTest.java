import com.caiyi.spark.util.NumberUtils;

import java.text.NumberFormat;

/**
 * Created by root on 2016/12/26.
 */
public class NumberFormatTest {

    public static void main(String[] args) {
        Integer num1 = 2;
        Integer num2 = 0;

        NumberFormat numberFormat = NumberFormat.getInstance();

        numberFormat.setMaximumFractionDigits(2);

        Double num = NumberUtils.formatDouble((double) num1 / (double) (num2) * 100, 2);

        System.out.println("=================" + num);

    }
}
