/**
 * Created by root on 2016/12/21.
 */
public class punctuationRemove {
    public static void main(String[] args) {
        String str = ",.!，，D_NAME。！；‘’”“《》**dfs  #$%^&()-+1431221中国123漢字かどうかのjavaを決定";

        String aaa = str.replaceAll("[\\pP‘’“”]","");
        System.out.println(aaa+"\n");

        str = str.replaceAll("[\\p{Space}]\\pP‘’“”","");
        System.out.println(str+"\n");

        str = str.replaceAll("[\\p{Space}\\pP‘’“”\\p{Punct}]","");
        System.out.println(str+"\n");

        String[] arr =
                {"获取账单失败，失败原因[登录失败-您输入的查询密码和信用卡号不匹配，连续6次输入错误将锁定账户]',",
                "信用卡卡号格式不正确', ",
                "信用卡未激活,请先去交!行柜面或??到交行信用卡中心网站激活!',",
                "对不起您已超过密码错误最>大次数',",
                "获取账单失败，失败原因[登录失败-登录名错误]',",
                "网银登录密码必须为6位的数字&**密码',",
                "登录名错误',",
                "您输入的查询密‘码和信用卡号$不匹配，连续6次输入错误将锁定账户',",
                "您%#的登录密码输入已超过限制，请24小时后再试。',",
                "密码输入不正确，请重新输入',",};

        System.out.println(arr.length);

        for(String aa:arr){
            aa = aa.replaceAll("[\\pP‘’“”\\p{Space}\\p{Punct}]", "");
            System.out.printf(aa+"\n");
        }
    }
}
