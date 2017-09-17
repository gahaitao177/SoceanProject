package bill;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by root on 2017/1/24.
 */
public class test1 {
    public static void main(String[] args) {
        Set<String> set1 = new TreeSet<String>();
        set1.add("page2");
        set1.add("page1");
        set1.add("page3");

        for (Iterator<String> stt = set1.iterator(); stt.hasNext();) {
            System.out.println("------" + stt.next());
        }
    }
}
