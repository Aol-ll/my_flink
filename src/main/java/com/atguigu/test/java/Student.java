package com.atguigu.test.java;

/**
 * @author Aol
 * @create 2021-12-19 10:56
 */
public class Student {
    String name;
    int age;

    public Student() {
    }

    public void eat() {
        System.out.println("吃饭");
    }

    public Student(String name, int age) {
        this.name = name;
        this.age = age;
    }
}
