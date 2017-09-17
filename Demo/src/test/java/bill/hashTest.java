package bill;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * Created by root on 2017/2/9.
 */
public class hashTest {

    public static void main(String[] args) {
        String aa = "20170201";
        String aa1 = "20170202";
        String aa2 = "20170203";

        int a1 = aa.hashCode();
        int a2 = aa1.hashCode();
        int a3 = aa2.hashCode();

        System.out.println("a1=" + a1 + " a2=" + a2 + " a3=" + a3);

        HashFunction hf = Hashing.murmur3_128();
        String str = "test";
        System.out.println(hf.newHasher().putString(str, Charsets.UTF_8).hash().asInt());

        Hasher hasher = hf.newHasher();
        System.out.println(hasher.putString(str, Charsets.UTF_8).hash().asInt());
        System.out.println(hasher.putString(str, Charsets.UTF_8).hash().asInt());
    }
}
