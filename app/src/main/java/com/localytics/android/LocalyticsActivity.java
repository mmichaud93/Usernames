package com.localytics.android;

import android.app.Activity;
import android.os.Bundle;

/**
 * The base class for easily integrating localytics SDK into user's app.
 * Users can derive from this activity so they don't need to worry about the
 * basic setup of localytics session.
 */
public class LocalyticsActivity extends Activity
{
    protected LocalyticsSession mLocalyticsSession;

    @Override
    protected void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);
        mLocalyticsSession = new LocalyticsSession(this);
        mLocalyticsSession.open();
        mLocalyticsSession.upload();
    }

    @Override
    public void onResume()
    {
        super.onResume();
        mLocalyticsSession.open();
        mLocalyticsSession.upload();
    }

    @Override
    public void onPause()
    {
        mLocalyticsSession.close();
        mLocalyticsSession.upload();
        super.onPause();
    }
}
