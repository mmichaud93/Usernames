package com.io.usernames.util;

import android.content.Context;
import android.util.DisplayMetrics;

/**
 * Created by michaudm3 on 10/28/2014.
 */
public class Utility {
    public static int dpToPx(Context context, int dp) {
        if(context!=null) {
            DisplayMetrics displayMetrics = context.getResources().getDisplayMetrics();
            int px = Math.round(dp * (displayMetrics.xdpi / DisplayMetrics.DENSITY_DEFAULT));
            return px;
        } else {
            return -1;
        }

    }

    public static int pxToDp(Context context, int px) {
        if(context!=null) {
            DisplayMetrics displayMetrics = context.getResources().getDisplayMetrics();
            int dp = Math.round(px / (displayMetrics.xdpi / DisplayMetrics.DENSITY_DEFAULT));
            return dp;
        } else {
            return -1;
        }
    }
}
