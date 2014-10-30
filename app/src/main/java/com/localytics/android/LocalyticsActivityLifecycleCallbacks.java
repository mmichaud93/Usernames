package com.localytics.android;

import android.annotation.TargetApi;
import android.app.Activity;
import android.app.Application;
import android.content.Intent;
import android.os.Bundle;
import android.support.v4.app.FragmentActivity;

/**
 * LocalyticsActivityLifecycleCallbacks class for one-line integration if
 * the user's app only targets to ICS and above.
 */
@TargetApi(14)
public class LocalyticsActivityLifecycleCallbacks implements Application.ActivityLifecycleCallbacks
{
    // Class name for LocalyticsAmpSession
    private final static String AMP_SESSION_CLASS_NAME = "com.localytics.android.LocalyticsAmpSession";

    // An instance of LocalyticsSession or LocalyticsAmpSession.
    private LocalyticsSession mSession;

    /**
     * Constructor of LocalyticsActivityLifecycleCallbacks.
     *
     * @param session The instance of LocalyticsSession or LocalyticsAmpSession, cannot be null.
     */
    public LocalyticsActivityLifecycleCallbacks(final LocalyticsSession session)
    {
        if (null == session)
        {
            throw new IllegalArgumentException("session cannot be null"); //$NON-NLS-1$
        }

        mSession = session;
    }

    @Override
    public void onActivityCreated(Activity activity, Bundle savedInstanceState)
    {
        if (mSession.getClass().getCanonicalName().equals(AMP_SESSION_CLASS_NAME))
        {
            if (!(activity instanceof FragmentActivity))
            {
                throw new IllegalArgumentException("LocalyticsAmpSession needs FragmentActivity as your activity's base class"); //$NON-NLS-1$
            }
        }

        mSession.open();
        mSession.upload();
        mSession.handlePushReceived(activity.getIntent());
        if (mSession.getClass().getCanonicalName().equals(AMP_SESSION_CLASS_NAME))
        {
            ReflectionUtils.tryInvokeInstance(mSession, "handleIntent", new Class<?>[]{Intent.class}, new Object[]{activity.getIntent()});
        }
    }

    @Override
    public void onActivityStarted(Activity activity)
    {
    }

    @Override
    public void onActivityResumed(Activity activity)
    {
        mSession.open();
        mSession.upload();
        if (mSession.getClass().getCanonicalName().equals(AMP_SESSION_CLASS_NAME))
        {
            if (activity instanceof FragmentActivity)
            {
                ReflectionUtils.tryInvokeInstance(mSession, "attach", new Class<?>[]{FragmentActivity.class}, new Object[]{(FragmentActivity) activity});
            }
            ReflectionUtils.tryInvokeInstance(mSession, "handleIntent", new Class<?>[]{Intent.class}, new Object[]{activity.getIntent()});
        }
        mSession.handlePushReceived(activity.getIntent());
    }

    @Override
    public void onActivityPaused(Activity activity)
    {
        if (mSession.getClass().getCanonicalName().equals(AMP_SESSION_CLASS_NAME))
        {
            if (activity instanceof FragmentActivity)
            {
                ReflectionUtils.tryInvokeInstance(mSession, "detach", null, null);
            }
        }
        mSession.close();
        mSession.upload();
    }

    @Override
    public void onActivityStopped(Activity activity)
    {
    }

    @Override
    public void onActivitySaveInstanceState(Activity activity, Bundle outState)
    {
    }

    @Override
    public void onActivityDestroyed(Activity activity)
    {
    }
}
