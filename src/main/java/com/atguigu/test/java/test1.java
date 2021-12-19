package com.atguigu.test.java;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Aol
 * @create 2021-12-18 15:55
 */
public class test1 {
    public static void main(String[] args) {
        System.out.println("你好");

        int value = 5;
        System.out.println(value);

        int num = 234;
        int bai = num / 100;
        int shi = num % 100 / 10;
        int ge = num % 10;

//        ge += bai;
        System.out.println("百位为" + bai + "十位为" + shi + "个位为" + ge);


//        Scanner scanner = new Scanner(System.in);
//        int nextInt = scanner.nextInt();
//
//        switch (nextInt){
//            case 100 :
//                System.out.println("111");
//                break;
//            case 1000:
//                System.out.println("222");
//            default:
//                System.out.println("333");
//        }

        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(4);
        list.add(2);
        System.out.println(list);
        System.out.println(list.stream().distinct().max(Integer::compareTo));

        System.out.println(list.stream().filter(integer -> integer ==2).collect(Collectors.toList()));

        System.out.println(list.stream().limit(2).collect(Collectors.toList()));

//        for (int i = 0; i < list.size(); i++) {
//            System.out.println(list.get(i));
//        }
//
//        list.forEach(System.out::println);

        for (int i:list){
            System.out.println(i);
        }

        System.out.println(getNum(10));

        Student student = new Student();

        student.eat();

        Random random = new Random();
        int i = random.nextInt(10);
        System.out.println(i);

        System.out.println(Math.min(1, 2));

//        while (true){
//            Scanner scanner = new Scanner(System.in);
//            int nextInt = scanner.nextInt();
//            if (num > nextInt) System.out.println("666");
//            else System.out.println("eee");
//        }

    }

     static int getNum(int i){
        if (i ==   1) return 1;
        else return i + getNum(i-1);
     }
}
