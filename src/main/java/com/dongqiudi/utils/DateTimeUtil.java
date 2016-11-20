package com.dongqiudi.utils;

import java.util.Date;

/**
 * Created by Joshua on 16/11/17.
 */
public class DateTimeUtil {
    public static long getTimestamp(){
        return new Date().getTime();
    }
}
