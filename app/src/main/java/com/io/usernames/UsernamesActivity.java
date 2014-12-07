package com.io.usernames;

import android.content.Context;
import android.os.Bundle;
import android.support.v7.app.ActionBar;
import android.support.v7.app.ActionBarActivity;
import android.view.LayoutInflater;
import android.view.View;

import com.google.android.gms.analytics.GoogleAnalytics;

import de.keyboardsurfer.android.widget.crouton.Crouton;


public class UsernamesActivity extends ActionBarActivity {

    public static String TRY_ANOTHER_EVENT = "Try Another Pressed";
    public static String USERNAME_EVENT = "Username Loaded";
    public static String URL_EVENT = "URL Clicked";
    public static String FACEBOOK_EVENT = "Facebook Clicked";
    public static String TWITTER_EVENT = "Twitter Clicked";
    public static String GITHUB_EVENT = "Github Clicked";
    public static String LINKEDIN_EVENT = "LinkedIn Clicked";
    /*
     * A swipe right means the user looked at one that was already generated
     * A swipe left means they went forward in time
     */
    public static String SWIPE_LEFT_EVENT = "Swiped Left";
    public static String SWIPE_RIGHT_EVENT = "Swiped Right";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        ((UsernamesApplication) getApplication()).getTracker(UsernamesApplication.TrackerName.APP_TRACKER);

        setContentView(R.layout.activity_usernames);

        ActionBar actionBar = getSupportActionBar();
        actionBar.setDisplayShowCustomEnabled(true);
        actionBar.setDisplayShowTitleEnabled(false);

        LayoutInflater inflator = (LayoutInflater) this .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        View customActionBar = inflator.inflate(R.layout.actionbar, null);

        actionBar.setCustomView(customActionBar);

        if (savedInstanceState == null) {
            getSupportFragmentManager().beginTransaction()
                    .add(R.id.container, new UsernamesFragment())
                    .commit();
        }
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        Crouton.cancelAllCroutons();
    }

    @Override
    public void onStart() {
        super.onStart();
        GoogleAnalytics.getInstance(this).reportActivityStart(this);
    }
    @Override
    public void onStop() {
        super.onStop();
        GoogleAnalytics.getInstance(this).reportActivityStop(this);
    }
}
