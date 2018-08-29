package com.cm.data.util;

import com.cm.data.constant.Constants;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 工具类
 */
public class StringUtils {

    /**
     * 判断 str 是否为空
     *
     * @param str 入参
     * @return 空为 false
     */
    public static boolean isNotEmpty(String str) {
        return !"".equals(str) && null != str;
    }

    /**
     * 根据入参动态创建分区数
     *
     * @param input      入参
     * @param columnName 指定分区字段
     * @return 对入参处理之后的分区值
     */
    public static String[] dynamicPartitions(String input, String columnName) {
        String array[] = null;

        if (isNotEmpty(input)) {
            String[] inputStr = input.split(",");
            long[] columnArray = new long[inputStr.length];

            if (isNotEmpty(columnName) && "id".equals(columnName)) {
                for (int i = 0; i < inputStr.length; i++) {
                    columnArray[i] = Integer.valueOf(inputStr[i]);
                }
            }
            if (isNotEmpty(columnName) && "createtime".equals(columnName)) {
                for (int i = 0; i < inputStr.length; i++) {
                    columnArray[i] = getTimeStamp(inputStr[i]);
                }
            }

            QuickSort.quickSort(columnArray, 0, columnArray.length - 1);

            if (columnArray.length != 0) {
                array = new String[columnArray.length / 2];
                for (int j = 0; j < columnArray.length; j += 2) {
                    array[j / 2] = columnName + " >= " + columnArray[j] + " and " + columnName + " <= " + columnArray[j + 1];
                }
            }
        }

        return array;
    }

    /**
     * 根据日期时间生成时间戳格式
     *
     * @param datetime 日期时间
     * @return 时间戳
     */
    public static long getTimeStamp(String datetime) {
        if (isNotEmpty(datetime)) {
            Calendar calendar = Calendar.getInstance();
            int currentYear = calendar.get(Calendar.YEAR);

            SimpleDateFormat formatter = new SimpleDateFormat(Constants.FULL_FORMAT);
            ParsePosition pos = new ParsePosition(0);

            return formatter.parse(currentYear + datetime, pos).getTime() / 1000;
        }

        return 0;
    }

    /**
     * 根据步长参数自动扩展起止范围内的字符串
     * @param start 起始参数
     * @param end   结束参数
     * @param step  步长参数
     * @return  起止范围内的字符串
     */
    public static String autoExpansion(int start, int end, int step) {
        String str = "";
        int k = step;
        for (int i = start, j = start; i <= end; i += step, j++) {
            if (j > start) {
                k += step;
            }
            str += i + "," + k + ",";
        }

        return str.substring(0,str.lastIndexOf(","));
    }

    /**
     * 普通数据加密
     * @param str   加密对象字符
     * @param index 加密对象字符下标
     * @return  加密后对象字符
     */
    public static String encrypt(String str, int index) {
        if (isNotEmpty(str) && !"0".equals(str) && !str.equals(0)) {
            if (index == 1) {
                return str.replace(str.substring(index, str.length()), "***");
            } else {
                if (str.length() <= index || str.substring(index, str.length()).length() <= index) {
                    return str;
                }
                return str.replace(str.substring(index, index + 4), "****");
            }
        }

        return "";
    }

    /**
     * SHA-256  数据加密
     * @param str   加密对象字符
     * @return  加密后对象字符
     */
    public static String encrypt(String str) {
        if (isNotEmpty(str)) {
            MessageDigest messageDigest;
            String encodeStr = "";
            try {
                messageDigest = MessageDigest.getInstance("SHA-256");
                byte[] hash = messageDigest.digest(str.getBytes("UTF-8"));
                encodeStr = Hex.encodeHexString(hash);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return encodeStr;
        }

        return "";
    }

}
