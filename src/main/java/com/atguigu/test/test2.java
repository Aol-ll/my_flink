package com.atguigu.test;

import java.util.Arrays;

/**
 * @author Aol
 * @create 2021-04-01 10:39
 */
public class test2 {
    public static void main(String[] args) {
        int[] arr1 = {3, 2, 1, 5, 6, 4};
        Arrays.sort(arr1);
        int k1 = 2;
        System.out.println(arr1[arr1.length -k1]);

        int[] arr2 = {3,2,3,1,2,4,5,5,6};
        Arrays.sort(arr2);
        int k2 = 4;
        System.out.println(arr2[arr2.length -k2]);

        int[] arr = {3, 2, 1, 5, 6, 4};
        Arrays.sort(arr);
        int k = 2;
        System.out.println(arr[arr.length -k]);

    }
}
