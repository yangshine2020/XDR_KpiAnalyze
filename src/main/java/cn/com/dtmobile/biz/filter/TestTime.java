package cn.com.dtmobile.biz.filter;

public class TestTime {

    public static void main(String[] args){
        String str = "2020-07-15 05:36:11.670792";
        String hour = str.replace("-", "").replace(" ", "").substring(0, 10);
        System.out.println(hour);
        String mcc = "460";
        String mnc = "01";

        String s = mcc + "-" + mnc;
        System.out.println(s);


        long e = Long.parseLong("42743058");
        String e1 = String.valueOf((int)Math.floor(e / 256));
        String e2 = String.valueOf((int)Math.floor(e % 256));
        String cgi = "460" + "-" + "01" + "-" + e1 + "-" + e2;
        System.out.println(cgi);

        String[] arr = {"a","b","c"};
        String s1 = arr.toString().length() + "\001" + "44" + "\001" + "33";
        String a = arr[0] + "\001" + "44" + "\001" + "33";
        System.out.println(s1);

    }
}
