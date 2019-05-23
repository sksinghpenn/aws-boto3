package helloworld;


import java.util.ArrayList;
import java.util.List;

public class HelloWorld {

    public static void main(String[] args) {
        List<Integer> a = new ArrayList<>();


        a.add(1);
        a.add(2);

        List<Integer> b = a;

        System.out.println(b);
        System.out.println(a);


    }

}
