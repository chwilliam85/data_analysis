package com.cm.data.util;

/**
 * 快速排序
 */
public class QuickSort {

    private static int division(long [] list, int left, int right){

        long base = list[left];
        while (left < right) {

            while (left < right && list[right] >= base) {
                right --;
            }
            list[left] = list[right];

            while (left < right && list[left] <= base){
                left ++;
            }
            list[right] = list[left];
        }

        list[left] = base;

        return left;
    }

    public static void quickSort(long [] list, int left, int right) {
        if (left < right) {

            int base = division(list, left, right);

            quickSort(list, left, base - 1);

            quickSort(list, base + 1, right);
        }
    }
}
