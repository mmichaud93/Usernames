package com.io.usernames.util;

import android.util.Log;

/**
 * Created by matthewmichaud on 10/23/14.
 */
public class UsernameLog {

    private static final String TAG = "Username";

    public static void d(String tag, String message) {
        Log.d(TAG, "[" + tag + "] " + message);
    }
    public static void i(String tag, String message) {
        Log.i(TAG, "[" + tag + "] " + message);
    }
    public static void e(String tag, String message) {
        Log.e(TAG, "[" + tag + "] " + message);
    }
    public static void v(String tag, String message) {
        Log.v(TAG, "[" + tag + "] " + message);
    }
    public static void w(String tag, String message) {
        Log.w(TAG, "[" + tag + "] " + message);
    }
}
