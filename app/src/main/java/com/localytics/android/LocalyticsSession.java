// @formatter:off
/*
 * LocalyticsSession.java Copyright (C) 2013 Char Software Inc., DBA Localytics. This code is provided under the Localytics
 * Modified BSD License. A copy of this license has been distributed in a file called LICENSE with this source code. Please visit
 * www.localytics.com for more information.
 */
// @formatter:on

package com.localytics.android;

import android.Manifest.permission;
import android.content.Context;
import android.content.Intent;
import android.location.Location;
import android.os.HandlerThread;
import android.os.Looper;
import android.text.TextUtils;
import android.util.Log;

import com.localytics.android.LocalyticsProvider.AttributesDbColumns;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * This class manages creating, collecting, and uploading a Localytics session. Please see the following guides for information on
 * how to best use this library, sample code, and other useful information:
 * <ul>
 * <li><a href="http://wiki.localytics.com/index.php?title=Developer's_Integration_Guide">Main Developer's Integration Guide</a></li>
 * <li><a href="http://wiki.localytics.com/index.php?title=Android_2_Minute_Integration">Android 2 minute integration Guide</a></li>
 * <li><a href="http://wiki.localytics.com/index.php?title=Android_Integration_Guide">Android Integration Guide</a></li>
 * </ul>
 * <p>
 * Permissions required:
 * <ul>
 * <li>{@link permission#INTERNET}</li> - Necessary to upload data to the webservice.</li>
 * </ul>
 * Permissions recommended:
 * <ul>
 * <li>{@link permission#ACCESS_WIFI_STATE}</li> - Necessary to identify the type of network connection the user has. Without this
 * permission, users connecting via Wi-Fi will be reported as having a connection type of 'unknown.'</li>
 * </ul>
 * <strong>Basic Usage</strong>
 * <ol>
 * <li>In {@code Activity#onCreate(Bundle)}, instantiate a {@link LocalyticsSession} object and assign it to a global variable in
 * the Activity (e.g. {@code mLocalyticsSession}).</li>
 * <li>In {@code Activity#onResume()}, call {@link #open()} or {@link #open(List)}.</li>
 * <li>In {@code Activity#onResume()}, consider calling {@link #upload()}. Because the session was just opened, this upload will
 * submit that open to the server and allow you to capture real-time usage of your application.</li>
 * <li>In {@code Activity#onResume()}, consider calling {@link #tagScreen(String)} to note that the user entered the Activity.
 * Assuming your application uses multiple Activities for navigation (rather than a single Activity with multiple Fragments}, this
 * will capture the flow of users as they move from Activity to Activity. Don't worry about Activity re-entrance. Because
 * {@code Activity#onResume()} can be called multiple times for different reasons, the Localytics library manages duplicate
 * {@link #tagScreen(String)} calls for you.</li>
 * <li>As the user interacts with your Activity, call {@link #tagEvent(String)}, {@link #tagEvent(String, Map)} or
 * {@link #tagEvent(String, Map, List)} to collect usage data.</li>
 * <li>In {@code Activity#onPause()}, call {@link #close()} or {@link #close(List)}.</li>
 * </ol>
 * <strong>Notes</strong>
 * <ul>
 * <li>Do not call any {@link LocalyticsSession} methods inside a loop. Instead, calls such as {@link #tagEvent(String)} should
 * follow user actions. This limits the amount of data which is stored and uploaded.</li>
 * <li>This library will create a database called "com.android.localytics.sqlite" within the host application's
 * {@link Context#getDatabasePath(String)} directory. For security, this file directory will be created
 * {@link Context#MODE_PRIVATE}. The host application must not modify this database file. If the host application implements a
 * backup/restore mechanism, such as {@code android.app.backup.BackupManager}, the host application should not worry about backing
 * up the data in the Localytics database.</li>
 * <li>This library is thread-safe but is not multi-process safe. Unless the application explicitly uses different process
 * attributes in the Android Manifest, this is not an issue. If you need to use multiple processes, then each process should have
 * its own Localytics API key in order to make data processing thread-safe.</li>
 * </ul>
 *
 * @version 2.0
 */
public class LocalyticsSession
{
    /*
     * DESIGN NOTES
     *
     * The LocalyticsSession stores all of its state as a SQLite database in the parent application's private database storage
     * directory.
     *
     * Every action performed within (open, close, opt-in, opt-out, customer events) are all treated as events by the library.
     * Events are given a package prefix to ensure a namespace without collisions. Events internal to the library are flagged with
     * the Localytics package name, while events from the customer's code are flagged with the customer's package name. There's no
     * need to worry about the customer changing the package name and disrupting the naming convention, as changing the package
     * name means that a new user is created in Android and the app with a new package name gets its own storage directory.
     *
     *
     * MULTI-THREADING
     *
     * The LocalyticsSession stores all of its state as a SQLite database in the parent application's private database storage
     * directory. Disk access is slow and can block the UI in Android, so the LocalyticsSession object is a wrapper around a pair
     * of Handler objects, with each Handler object running on its own separate thread.
     *
     * All requests made of the LocalyticsSession are passed along to the mSessionHandler object, which does most of the work. The
     * mSessionHandler will pass off upload requests to the mUploadHandler, to prevent the mSessionHandler from being blocked by
     * network traffic.
     *
     * If an upload request is made, the mSessionHandler will set a flag that an upload is in progress (this flag is important for
     * thread-safety of the session data stored on disk). Then the upload request is passed to the mUploadHandler's queue. If a
     * second upload request is made while the first one is underway, the mSessionHandler notifies the mUploadHandler, which will
     * notify the mSessionHandler to retry that upload request when the first upload is completed.
     *
     * Although each LocalyticsSession object will have its own unique instance of mSessionHandler, thread-safety is handled by
     * using a single sSessionHandlerThread.
     */
    
    /**
     * Push Opened event
     */
    /* package */static final String PUSH_OPENED_EVENT = "Localytics Push Opened"; //$NON-NLS-1$    

    /**
     * Campaign ID attribute
     */
    /* package */static final String CAMPAIGN_ID_ATTRIBUTE = "Campaign ID"; //$NON-NLS-1$    

    /**
     * Creative ID attribute
     */
    /* package */static final String CREATIVE_ID_ATTRIBUTE = "Creative ID"; //$NON-NLS-1$    
    
    /**
     * Enumeration of actions possible to modify a customer's profile data
     */
    static enum ProfileDbAction {SET_ATTRIBUTE};

    /**
     * Background thread used for all Localytics session processing. This thread is shared across all instances of
     * LocalyticsSession within a process.
     */
    /*
     * By using the class name for the HandlerThread, obfuscation through Proguard is more effective: if Proguard changes the
     * class name, the thread name also changes.
     */
    protected static final HandlerThread sSessionHandlerThread = getHandlerThread(SessionHandler.class.getSimpleName());    

    /**
     * Helper to obtain a new {@link HandlerThread}.
     *
     * @param name to give to the HandlerThread. Useful for debugging, as the thread name is shown in DDMS.
     * @return HandlerThread whose {@link HandlerThread#start()} method has already been called.
     */
    private static HandlerThread getHandlerThread(final String name)
    {
        final HandlerThread thread = new HandlerThread(name, android.os.Process.THREAD_PRIORITY_BACKGROUND);

        thread.start();

        /*
         * Note: we tried setting an uncaught exception handler here. But for some reason it causes looper initialization to fail
         * randomly.
         */

        return thread;
    }

    /**
     * Maps an API key to a singleton instance of the {@link SessionHandler}. Lazily initialized during construction of the
     * {@link LocalyticsSession} object.
     */
    private static final Map<String, SessionHandler> sLocalyticsSessionHandlerMap = new HashMap<String, SessionHandler>();

    /**
     * Intrinsic lock for synchronizing the initialization of {@link #sLocalyticsSessionHandlerMap}.
     */
    private static final Object[] sLocalyticsSessionIntrinsicLock = new Object[0];

    /**
     * Handler object where all session requests of this instance of LocalyticsSession are handed off to.
     * <p>
     * This Handler is the key thread synchronization point for all work inside the LocalyticsSession.
     * <p>
     * This handler runs on {@link #sSessionHandlerThread}.
     */
    private final SessionHandler mSessionHandler;
    
    /**
     * Creates a new Handler that runs on {@code looper}.
     *
     * @param context Application context. Cannot be null.
     * @param appKey Localytics APP key. Cannot be null.
     * @param looper to run the Handler on. Cannot be null.
     */
    protected SessionHandler createSessionHandler(final Context context, final String appKey, final Looper looper)
    {
        SessionHandler handler = null;
        try
        {
            handler = ReflectionUtils.tryInvokeConstructor(
                    "com.localytics.android.AmpSessionHandler",
                    new Class<?>[]{Context.class, String.class, Looper.class},
                    new Object[]{context, appKey, looper});

            if (null == handler)
            {
                throw new Exception();
            }
        }
        catch (final Exception e)
        {
            handler = new SessionHandler(context, appKey, looper);
        }
        finally
        {
            return handler;
        }
    }
    
    /**
     * Gets the session handler that runs on {@code looper}.
     * TODO:
     * @return 
     */
    /* package */SessionHandler getSessionHandler()
    {
    	return mSessionHandler;
    }

    /**
     * Application context
     */
    protected final Context mContext;

    /**
     * Constructs a new {@link LocalyticsSession} object.
     *
     * @param context The context used to access resources on behalf of the app. It is recommended to use
     *            {@link Context#getApplicationContext()} to avoid the potential memory leak incurred by maintaining references to
     *            {@code Activity} instances. Cannot be null.
     * @throws IllegalArgumentException if {@code context} is null
     * @throws IllegalArgumentException if LOCALYTICS_APP_KEY in AndroidManifest.xml is null or empty
     */
    public LocalyticsSession(final Context context)
    {
    	this(context, null);
    }
    
    /**
     * Constructs a new {@link LocalyticsSession} object.
     *
     * @param context The context used to access resources on behalf of the app. It is recommended to use
     *            {@link Context#getApplicationContext()} to avoid the potential memory leak incurred by maintaining references to
     *            {@code Activity} instances. Cannot be null.
     * @param key The key unique for each application generated at www.localytics.com. Cannot be null or empty.
     * @throws IllegalArgumentException if {@code context} is null
     * @throws IllegalArgumentException if {@code key} is null or empty
     */
    public LocalyticsSession(final Context context, final String key)
    {
        this(context, key, null);
    }

    /**
     * Constructs a new {@link LocalyticsSession} object.
     *
     * @param context The context used to access resources on behalf of the app. It is recommended to use
     *            {@link Context#getApplicationContext()} to avoid the potential memory leak incurred by maintaining references to
     *            {@code Activity} instances. Cannot be null.
     * @param key The key unique for each application generated at www.localytics.com. Cannot be null or empty.
     * @param referrerID Referred ID for when created from ReferralReceiver
     * @throws IllegalArgumentException if {@code context} is null
     * @throws IllegalArgumentException if {@code key} is null or empty
     */
    /* package */LocalyticsSession(final Context context, final String key, final String referrerID)
    {
        if (context == null)
        {
            throw new IllegalArgumentException("context cannot be null"); //$NON-NLS-1$
        }
        
        String appKey = key;
        if (TextUtils.isEmpty(appKey))
        {
        	appKey = DatapointHelper.getLocalyticsAppKeyOrNull(context);
        }
        
        if (TextUtils.isEmpty(appKey))
        {
        	throw new IllegalArgumentException("key cannot be null or empty"); //$NON-NLS-1$
        }

        /*
         * Prevent the client from providing a subclass of Context that returns the Localytics package name.
         *
         * Note that because getPackageName() is a method and could theoretically return different results with each invocation,
         * this check doesn't guarantee that a nefarious caller will be detected.
         */
        if (Constants.LOCALYTICS_PACKAGE_NAME.equals(context.getPackageName())
                && !context.getClass().getName().equals("android.test.IsolatedContext") && !context.getClass().getName().equals("android.test.RenamingDelegatingContext")) //$NON-NLS-1$ //$NON-NLS-2$
        {
            throw new IllegalArgumentException(String.format("context.getPackageName() returned %s", context.getPackageName())); //$NON-NLS-1$
        }

        /*
         * Get the application context to avoid having the Localytics object holding onto an Activity object. Using application
         * context is very important to prevent the customer from giving the library multiple different contexts with different
         * package names, which would corrupt the events in the database.
         *
         * Although RenamingDelegatingContext is part of the Android SDK, the class isn't present in the ClassLoader unless the
         * process is being run as a unit test. For that reason, comparing class names is necessary instead of doing instanceof.
         *
         * Note that getting the application context may have unpredictable results for apps sharing a process running Android 2.1
         * and earlier. See <http://code.google.com/p/android/issues/detail?id=4469> for details.
         */
        mContext = !(context.getClass().getName().equals("android.test.RenamingDelegatingContext")) && Constants.CURRENT_API_LEVEL >= 8 ? context.getApplicationContext() : context; //$NON-NLS-1$

        synchronized (sLocalyticsSessionIntrinsicLock)
        {
            SessionHandler handler = sLocalyticsSessionHandlerMap.get(appKey);

            if (null == handler)
            {
                handler = createSessionHandler(mContext, appKey, sSessionHandlerThread.getLooper());
                sLocalyticsSessionHandlerMap.put(appKey, handler);
            }
            /*
             * Complete Handler initialization on a background thread. Note that this is not generally a good best practice,
             * as the LocalyticsSession object (and its child objects) should be fully initialized by the time the constructor
             * returns. However this implementation is safe, as the Handler will process this initialization message before
             * any other message.
             */
            handler.sendMessage(handler.obtainMessage(SessionHandler.MESSAGE_INIT, referrerID));

            mSessionHandler = handler;
        }
    }

    /**
     * Sets the Localytics opt-out state for this application. This call is not necessary and is provided for people who wish to
     * allow their users the ability to opt out of data collection. It can be called at any time. Passing true causes all further
     * data collection to stop, and an opt-out event to be sent to the server so the user's data is removed from the charts. <br>
     * There are very serious implications to the quality of your data when providing an opt out option. For example, users who
     * have opted out will appear as never returning, causing your new/returning chart to skew. <br>
     * If two instances of the same application are running, and one is opted in and the second opts out, the first will also
     * become opted out, and neither will collect any more data. <br>
     * If a session was started while the app was opted out, the session open event has already been lost. For this reason, all
     * sessions started while opted out will not collect data even after the user opts back in or else it taints the comparisons
     * of session lengths and other metrics.
     *
     * @param isOptedOut True if the user should be be opted out and have all his Localytics data deleted.
     */
    public void setOptOut(final boolean isOptedOut)
    {
        mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_OPT_OUT, isOptedOut ? 1 : 0, 0));
    }

    /**
     * Behaves identically to calling {@code open(null)}.
     *
     * @see #open(List)
     */
    public void open()
    {
        open(null);
    }

    /**
     * Opens the Localytics session. The session should be opened before {@link #tagEvent(String)}, {@link #tagEvent(String, Map)}
     * , {@link #tagEvent(String, Map, List)}, or {@link #tagScreen(String)} are called.
     * <p>
     * If a new session is opened shortly--within a few seconds--after an earlier session is closed, Localytics will reconnect to
     * the previous session (effectively causing the previous close to be ignored). This ensures that as a user moves from
     * Activity to Activity in an app, that is considered a single session. When a session is reconnected, the
     * {@code customDimensions} for the initial open are kept and dimensions for the second open are ignored.
     * <p>
     * If for any reason open is called more than once without an intervening call to {@link #close()} or {@link #close(List)},
     * subsequent calls to open will be ignored.
     *
     * @param customDimensions A set of custom reporting dimensions. If this parameter is null or empty, then no custom dimensions
     *            are recorded and the behavior with respect to custom dimensions is like simply calling {@link #open()}. The
     *            number of dimensions is capped at four. If there are more than four elements, the extra elements are ignored.
     *            This parameter may not contain null or empty elements. This parameter is only used for enterprise level
     *            accounts. For non-enterprise accounts, custom dimensions will be uploaded but will not be accessible in reports
     *            until the account is upgraded to enterprise status.
     * @throws IllegalArgumentException if {@code customDimensions} contains null or empty elements.
     */
    public void open(final List<String> customDimensions)
    {
        if (Constants.IS_PARAMETER_CHECKING_ENABLED)
        {
            if (null != customDimensions)
            {
                /*
                 * Calling this with empty dimensions is a smell that indicates a possible programming error on the part of the
                 * caller
                 */
                if (customDimensions.isEmpty())
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, "customDimensions is empty.  Did the caller make an error?"); //$NON-NLS-1$
                    }
                }

                if (customDimensions.size() > Constants.MAX_CUSTOM_DIMENSIONS)
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, String.format("customDimensions size is %d, exceeding the maximum size of %d.  Did the caller make an error?", Integer.valueOf(customDimensions.size()), Integer.valueOf(Constants.MAX_CUSTOM_DIMENSIONS))); //$NON-NLS-1$
                    }
                }

                for (final String element : customDimensions)
                {
                    if (null == element)
                    {
                        throw new IllegalArgumentException("customDimensions cannot contain null elements"); //$NON-NLS-1$
                    }
                    if (0 == element.length())
                    {
                        throw new IllegalArgumentException("customDimensions cannot contain empty elements"); //$NON-NLS-1$
                    }
                }
            }
        }

        if (null == customDimensions || customDimensions.isEmpty())
        {
            mSessionHandler.sendEmptyMessage(SessionHandler.MESSAGE_OPEN);
        }
        else
        {
            mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_OPEN, new TreeMap<String, String>(convertDimensionsToAttributes(customDimensions))));
        }
    }

    /**
     * Behaves identically to calling {@code close(null)}.
     *
     * @see #close(List)
     */
    public void close()
    {
        close(null);
    }

    /**
     * Closes the Localytics session. Once a session has been opened via {@link #open()} or {@link #open(List)}, close the session
     * when data collection is complete.
     * <p>
     * If close is called without open having ever been called, the close has no effect. Similarly, once a session is closed,
     * subsequent calls to close will be ignored.
     *
     * @param customDimensions A set of custom reporting dimensions. If this parameter is null or empty, then no custom dimensions
     *            are recorded and the behavior with respect to custom dimensions is like simply calling {@link #close()}. The
     *            number of dimensions is capped at four. If there are more than four elements, the extra elements are ignored.
     *            This parameter may not contain null or empty elements. This parameter is only used for enterprise level
     *            accounts. For non-enterprise accounts, custom dimensions will be uploaded but will not be accessible in reports
     *            until the account is upgraded to enterprise status.
     * @throws IllegalArgumentException if {@code customDimensions} contains null or empty elements.
     */
    public void close(final List<String> customDimensions)
    {
        if (Constants.IS_PARAMETER_CHECKING_ENABLED)
        {
            if (null != customDimensions)
            {
                /*
                 * Calling this with empty dimensions is a smell that indicates a possible programming error on the part of the
                 * caller
                 */
                if (customDimensions.isEmpty())
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, "customDimensions is empty.  Did the caller make an error?"); //$NON-NLS-1$
                    }
                }

                if (customDimensions.size() > Constants.MAX_CUSTOM_DIMENSIONS)
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, String.format("customDimensions size is %d, exceeding the maximum size of %d.  Did the caller make an error?", Integer.valueOf(customDimensions.size()), Integer.valueOf(Constants.MAX_CUSTOM_DIMENSIONS))); //$NON-NLS-1$
                    }
                }

                for (final String element : customDimensions)
                {
                    if (null == element)
                    {
                        throw new IllegalArgumentException("customDimensions cannot contain null elements"); //$NON-NLS-1$
                    }
                    if (0 == element.length())
                    {
                        throw new IllegalArgumentException("customDimensions cannot contain empty elements"); //$NON-NLS-1$
                    }
                }
            }
        }

        if (null == customDimensions || customDimensions.isEmpty())
        {
            mSessionHandler.sendEmptyMessage(SessionHandler.MESSAGE_CLOSE);
        }
        else
        {
            mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_CLOSE, new TreeMap<String, String>(convertDimensionsToAttributes(customDimensions))));
        }
    }

    /**
     * Behaves identically to calling {@code tagEvent(event, null, null, 0)}.
     *
     * @see #tagEvent(String, Map, List, long)
     * @param event The name of the event which occurred. Cannot be null or empty string.
     * @throws IllegalArgumentException if {@code event} is null.
     * @throws IllegalArgumentException if {@code event} is empty.
     */
    public void tagEvent(final String event)
    {
        tagEvent(event, null);
    }

    /**
     * Behaves identically to calling {@code tagEvent(event, attributes, null, 0)}.
     *
     * @see #tagEvent(String, Map, List, long)
     * @param event The name of the event which occurred. Cannot be null or empty string.
     * @param attributes The collection of attributes for this particular event. If this parameter is null or empty, then calling
     *            this method has the same effect as calling {@link #tagEvent(String)}. This parameter may not contain null or
     *            empty keys or values.
     * @throws IllegalArgumentException if {@code event} is null.
     * @throws IllegalArgumentException if {@code event} is empty.
     * @throws IllegalArgumentException if {@code attributes} contains null keys, empty keys, null values, or empty values.
     */
    public void tagEvent(final String event, final Map<String, String> attributes)
    {
        tagEvent(event, attributes, null);
    }

    /**
     * Behaves identically to calling {@code tagEvent(event, attributes, customDimensions, 0)}.
     *
     * @see #tagEvent(String, Map, List, long)
     * @param event The name of the event which occurred. Cannot be null or empty string.
     * @param attributes The collection of attributes for this particular event. If this parameter is null or empty, then calling
     *            this method has the same effect as calling {@link #tagEvent(String)}. This parameter may not contain null or
     *            empty keys or values.
     * @param customDimensions A set of custom reporting dimensions. If this parameter is null or empty, then no custom dimensions
     *            are recorded and the behavior with respect to custom dimensions is like simply calling {@link #tagEvent(String)}
     *            . The number of dimensions is capped at four. If there are more than four elements, the extra elements are
     *            ignored. This parameter may not contain null or empty elements. This parameter is only used for enterprise level
     *            accounts. For non-enterprise accounts, custom dimensions will be uploaded but will not be accessible in reports
     *            until the account is upgraded to enterprise status.
     * @throws IllegalArgumentException if {@code event} is null.
     * @throws IllegalArgumentException if {@code event} is empty.
     * @throws IllegalArgumentException if {@code attributes} contains null keys, empty keys, null values, or empty values.
     * @throws IllegalArgumentException if {@code customDimensions} contains null or empty elements.
     */
    public void tagEvent(final String event, final Map<String, String> attributes, final List<String> customDimensions)
    {
        tagEvent(event, attributes, customDimensions, 0);
    }
    
    /**
     * <p>
     * Within the currently open session, tags that {@code event} occurred (with optionally included attributes and dimensions).
     * </p>
     * <p>
     * Attributes: Additional key/value pairs with data related to an event. For example, let's say your app displays a dialog
     * with two buttons: OK and Cancel. When the user clicks on one of the buttons, the event might be "button clicked." The
     * attribute key might be "button_label" and the value would either be "OK" or "Cancel" depending on which button was clicked.
     * </p>
     * <p>
	 * Custom dimensions:
	 * (PREMIUM ONLY) Sets the value of a custom dimension. Custom dimensions are dimensions
	 * which contain user defined data unlike the predefined dimensions such as carrier, model, and country.
	 * The proper use of custom dimensions involves defining a dimension with less than ten distinct possible
	 * values and assigning it to one of the fogur available custom dimensions. Once assigned this definition should
	 * never be changed without changing the App Key otherwise old installs of the application will pollute new data.
	 * </p>
     * <strong>Best Practices</strong>
     * <ul>
     * <li>DO NOT use events, attributes, or dimensions to record personally identifiable information.</li>
     * <li>The best way to use events is to create all the event strings as predefined constants and only use those. This is more
     * efficient and removes the risk of collecting personal information.</li>
     * <li>Do not tag events inside loops or any other place which gets called frequently. This can cause a lot of data to be
     * stored and uploaded.</li>
     * </ul>
     *
     * @param event The name of the event which occurred. Cannot be null or empty string.
     * @param attributes The collection of attributes for this particular event. If this parameter is null or empty, then no
     *            attributes are recorded and the behavior with respect to attributes is like simply calling
     *            {@link #tagEvent(String)}. This parameter may not contain null or empty keys or values.
     * @param customDimensions A set of custom reporting dimensions. If this parameter is null or empty, then no custom dimensions
     *            are recorded and the behavior with respect to custom dimensions is like simply calling {@link #tagEvent(String)}
     *            . The number of dimensions is capped at four. If there are more than four elements, the extra elements are
     *            ignored. This parameter may not contain null or empty elements. This parameter is only used for enterprise level
     *            accounts. For non-enterprise accounts, custom dimensions will be uploaded but will not be accessible in reports
     *            until the account is upgraded to enterprise status.
     * @param customerValueIncrease Added to customer lifetime value. Try to use lowest possible unit, such as cents for US currency. 
     * @throws IllegalArgumentException if {@code event} is null.
     * @throws IllegalArgumentException if {@code event} is empty.
     * @throws IllegalArgumentException if {@code attributes} contains null keys, empty keys, null values, or empty values.
     * @throws IllegalArgumentException if {@code customDimensions} contains null or empty elements.
     */
    public void tagEvent(final String event, final Map<String, String> attributes, final List<String> customDimensions, final long customerValueIncrease)
    {
        if (Constants.IS_PARAMETER_CHECKING_ENABLED)
        {
            if (null == event)
            {
                throw new IllegalArgumentException("event cannot be null"); //$NON-NLS-1$
            }

            if (0 == event.length())
            {
                throw new IllegalArgumentException("event cannot be empty"); //$NON-NLS-1$
            }

            if (null != attributes)
            {
                /*
                 * Calling this with empty attributes is a smell that indicates a possible programming error on the part of the
                 * caller
                 */
                if (attributes.isEmpty())
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, "attributes is empty.  Did the caller make an error?"); //$NON-NLS-1$
                    }
                }

                if (attributes.size() > Constants.MAX_NUM_ATTRIBUTES)
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, String.format("attributes size is %d, exceeding the maximum size of %d.  Did the caller make an error?", Integer.valueOf(attributes.size()), Integer.valueOf(Constants.MAX_NUM_ATTRIBUTES))); //$NON-NLS-1$
                    }
                }

                for (final Entry<String, String> entry : attributes.entrySet())
                {
                    final String key = entry.getKey();
                    final String value = entry.getValue();

                    if (null == key)
                    {
                        throw new IllegalArgumentException("attributes cannot contain null keys"); //$NON-NLS-1$
                    }
                    if (null == value)
                    {
                        throw new IllegalArgumentException("attributes cannot contain null values"); //$NON-NLS-1$
                    }
                    if (0 == key.length())
                    {
                        throw new IllegalArgumentException("attributes cannot contain empty keys"); //$NON-NLS-1$
                    }
                    if (0 == value.length())
                    {
                        throw new IllegalArgumentException("attributes cannot contain empty values"); //$NON-NLS-1$
                    }
                }
            }

            if (null != customDimensions)
            {
                /*
                 * Calling this with empty dimensions is a smell that indicates a possible programming error on the part of the
                 * caller
                 */
                if (customDimensions.isEmpty())
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, "customDimensions is empty.  Did the caller make an error?"); //$NON-NLS-1$
                    }
                }

                if (customDimensions.size() > Constants.MAX_CUSTOM_DIMENSIONS)
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, String.format("customDimensions size is %d, exceeding the maximum size of %d.  Did the caller make an error?", Integer.valueOf(customDimensions.size()), Integer.valueOf(Constants.MAX_CUSTOM_DIMENSIONS))); //$NON-NLS-1$
                    }
                }

                for (final String element : customDimensions)
                {
                    if (null == element)
                    {
                        throw new IllegalArgumentException("customDimensions cannot contain null elements"); //$NON-NLS-1$
                    }
                    if (0 == element.length())
                    {
                        throw new IllegalArgumentException("customDimensions cannot contain empty elements"); //$NON-NLS-1$
                    }
                }
            }
        }

        final String eventString = String.format(Constants.EVENT_FORMAT, mContext.getPackageName(), event);

        if (null == attributes && null == customDimensions)
        {
            mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_TAG_EVENT, new Object[] { eventString, null, customerValueIncrease }));
        }
        else
        {
            /*
             * Convert the attributes and custom dimensions into the internal representation of packagename:key
             */
            final TreeMap<String, String> remappedAttributes = new TreeMap<String, String>();

            if (null != attributes)
            {
                final String packageName = mContext.getPackageName();
                for (final Entry<String, String> entry : attributes.entrySet())
                {
                    remappedAttributes.put(String.format(AttributesDbColumns.ATTRIBUTE_FORMAT, packageName, entry.getKey()), entry.getValue());
                }
            }

            if (null != customDimensions)
            {
                remappedAttributes.putAll(convertDimensionsToAttributes(customDimensions));
            }

            /*
             * Copying the map is very important to ensure that a client can't modify the map after this method is called. This is
             * especially important because the map is subsequently processed on a background thread.
             *
             * A TreeMap is used to ensure that the order that the attributes are written is deterministic. For example, if the
             * maximum number of attributes is exceeded the entries that occur later alphabetically will be skipped consistently.
             */

            mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_TAG_EVENT, new Object[] { eventString, new TreeMap<String, String>(remappedAttributes), customerValueIncrease }));
        }
    }

    /**
     * Note: This implementation will perform duplicate suppression on two identical screen events that occur in a row within a
     * single session. For example, in the set of screens {"Screen 1", "Screen 1"} the second screen would be suppressed. However
     * in the set {"Screen 1", "Screen 2", "Screen 1"}, no duplicate suppression would occur.
     *
     * @param screen Name of the screen that was entered. Cannot be null or the empty string.
     * @throws IllegalArgumentException if {@code event} is null.
     * @throws IllegalArgumentException if {@code event} is empty.
     */
    public void tagScreen(final String screen)
    {
        if (null == screen)
        {
            throw new IllegalArgumentException("event cannot be null"); //$NON-NLS-1$
        }

        if (0 == screen.length())
        {
            throw new IllegalArgumentException("event cannot be empty"); //$NON-NLS-1$
        }

        mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_TAG_SCREEN, screen));
    }

    /**
     * Record your custom customer email address.
     * Once this value is set, the device it was set on will continue to upload that value until the value is changed.
     * To delete the value, pass in nil.
     *
     * @param email The custom email address.
     */
    public void setCustomerEmail(final String email)
    {
    	setCustomerData("email", email);
    }

    /**
     * Record your custom customer name.
     * Once this value is set, the device it was set on will continue to upload that value until the value is changed.
     * To delete the value, pass in nil.
     *
     * @param name The custom name.
     */
    public void setCustomerName(final String name)
    {
    	setCustomerData("customer_name", name);
    }

    /**
     * Record your custom customer identifier.
     * Once this value is set, the device it was set on will continue to upload that value until the value is changed.
     * To delete the value, pass in nil.
     *
     * @param customerId The custom identifier.
     */
    public void setCustomerId(final String customerId)
    {
    	setCustomerData("customer_id", customerId);
    }
    
    public void setCustomerData(final String key, final String value)
    {
    	if(null == key)
    	{
    		throw new IllegalArgumentException("key cannot be null"); //$NON-NLS-1$
    	}
    	
    	mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_SET_IDENTIFIER, new Object[] { key, value }));
    }

    /**
     * Sets the value of a custom dimension. Custom dimensions are dimensions
     * which contain user defined data unlike the predefined dimensions such as carrier, model, and country.
     * Once a value for a custom dimension is set, the device it was set on will continue to upload that value
     * until the value is changed. To clear a value pass null as the value.
     * The proper use of custom dimensions involves defining a dimension with less than ten distinct possible
     * values and assigning it to one of the four available custom dimensions. Once assigned this definition should
     * never be changed without changing the App Key otherwise old installs of the application will pollute new data.
     *
     * @param dimension The given dimension between 0 and 9.
     * @param value The value to set for the given dimension
     */
    public void setCustomDimension(int dimension, final String value)
    {
        if (dimension < 0 || dimension > 9)
        {
            throw new IllegalArgumentException("Only valid dimensions are 0 - 9"); //$NON-NLS-1$
        }

        mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_SET_CUSTOM_DIMENSION, new Object[] { dimension, value == null ? null : new String(value) }));
    }

//    /**
//     * Gets the custom dimension value for a given dimension.
//     *
//     * @param dimension The given dimension
//     * @return the value of the given dimension.
//     */
//    public String getCustomDimension(int dimension)
//    {
//        return "";
//    }

    public void registerPush(final String senderId)
    {
    	if (DatapointHelper.getApiLevel() < 8)
    	{
    		if (Constants.IS_LOGGABLE)
    		{
    			Log.w(Constants.LOG_TAG, "GCM requires API level 8 or higher"); //$NON-NLS-1$
    		}
    	}

    	mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_REGISTER_PUSH, new String(senderId)));
    }

    public void handlePushReceived(final Intent intent)
    {
    	handlePushReceived(intent, null);
    }    
    
    public void handlePushReceived(final Intent intent, final List<String> customDimensions)
    {
        if (intent == null || intent.getExtras() == null) return;
        
        // Tag an event indicating the push was opened
        String llString = intent.getExtras().getString("ll");        
        if (llString != null)
        {
        	try 
        	{
        		JSONObject llObject = new JSONObject(llString);
        		String campaignId = llObject.getString("ca");
        		String creativeId = llObject.getString("cr");
        		
        		if (campaignId != null && creativeId != null)
        		{
        			HashMap<String, String> attributes = new HashMap<String, String>();
        			attributes.put(CAMPAIGN_ID_ATTRIBUTE, campaignId);
        			attributes.put(CREATIVE_ID_ATTRIBUTE, creativeId);
        			tagEvent(PUSH_OPENED_EVENT, attributes, customDimensions);
        		}
        		
        		// Remove the extra so we don't tag the same event a second time
        		intent.removeExtra("ll");
        	}
        	catch (JSONException e)
        	{
        		if (Constants.IS_LOGGABLE)
        		{
        			Log.w(Constants.LOG_TAG, "Failed to get campaign id or creative id from payload"); //$NON-NLS-1$
        		}
        	}
        }        
    }

    public void setPushRegistrationId(final String pushRegId)
    {
    	mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_SET_PUSH_REGID, pushRegId));
    }
    
    public void setLocation(Location location)
    {
    	mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_SET_LOCATION, new Location(location)));
    }

    public void setPushDisabled(final boolean disable)
    {
        mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_DISABLE_PUSH, disable ? 1 : 0, 0));
    }

    public void handleRegistration(final Intent intent)
    {
        mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_HANDLE_PUSH_REGISTRATION, intent));
    }

    public void handleNotificationReceived(Intent intent)
    {
        mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_HANDLE_PUSH_RECEIVED, intent));
    }
    
    /**
     * Set a customer profile attribute
     *
     * @param attributeName The attribute name
     * @param attributeValue The attribute value (can be null)
     */
    public void setProfileAttribute(String attributeName, Object attributeValue)
    {
        try
        {
            if (null == attributeName)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.e(Constants.LOG_TAG, "attribute name cannot be null"); //$NON-NLS-1$
                }
                return;
            }
            attributeName = attributeName.trim();
            if (0 == attributeName.length())
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.e(Constants.LOG_TAG, "attribute name cannot be empty"); //$NON-NLS-1$
                }
                return;
            }
            if (attributeName.getBytes().length > Constants.MAX_NAME_LENGTH)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.e(Constants.LOG_TAG, String.format("attribute name cannot be longer than %s characters", Constants.MAX_NAME_LENGTH)); //$NON-NLS-1$
                }
                return;
            }
            if (attributeName.startsWith("_"))
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.e(Constants.LOG_TAG, "attribute name cannot start with \"_\""); //$NON-NLS-1$
                }
                return;
            }

            // acceptable types
            // 1. String
            // 2. String[]
            // 3. int/Integer
            // 4. int[]/Integer[]
            // 5. long/Long
            // 6. long[]/Long[]
            // 7. Date
            // 8. Date[]
            // 9. null
            if ((null != attributeValue) &&
                    !(attributeValue instanceof String) &&
                    !(attributeValue instanceof String[]) &&
                    !(attributeValue instanceof Date) &&
                    !(attributeValue instanceof Date[]) &&
                    !(attributeValue instanceof Integer) &&
                    !(attributeValue instanceof int[]) &&
                    !(attributeValue instanceof Integer[]) &&
                    !(attributeValue instanceof Long) &&
                    !(attributeValue instanceof long[]) &&
                    !(attributeValue instanceof Long[]))
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.e(Constants.LOG_TAG, "Profile property value can only be one of the following data types: String, String[], Date, Date[], int/Integer, int[]/Integer[], long/Long, long[]/Long[], or null"); //$NON-NLS-1$
                }
                return;
            }

            final JSONObject json = new JSONObject();
            // recast null to JSONObject.NULL
            if (null == attributeValue)
            {
                attributeValue = JSONObject.NULL;
            }
            // recast Date to String
            else if (attributeValue instanceof Date)
            {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                attributeValue = simpleDateFormat.format(attributeValue);
            }
            // recast int[] to Integer[]
            else if (attributeValue instanceof int[])
            {
                int[] oldValue = (int[]) attributeValue;
                Integer[] newValue = new Integer[oldValue.length];
                for (int i = 0; i < oldValue.length; i++)
                {
                    newValue[i] = oldValue[i];
                }
                attributeValue = newValue;
            }
            // recast long[] to Long[]
            else if (attributeValue instanceof long[])
            {
                long[] oldValue = (long[]) attributeValue;
                Long[] newValue = new Long[oldValue.length];
                for (int i = 0; i < oldValue.length; i++)
                {
                    newValue[i] = oldValue[i];
                }
                attributeValue = newValue;
            }
            // recast Date[] to String[]
            else if (attributeValue instanceof Date[])
            {
                Date[] oldValue = (Date[]) attributeValue;
                String[] newValue = new String[oldValue.length];
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                for (int i = 0; i < oldValue.length; i++)
                {
                    if (null != oldValue[i])
                    {
                        String date = simpleDateFormat.format(oldValue[i]);
                        newValue[i] = date;
                    }
                }
                attributeValue = newValue;
            }
            else if (attributeValue instanceof String)
            {
                if (((String)attributeValue).getBytes().length > Constants.MAX_VALUE_LENGTH)
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.e(Constants.LOG_TAG, String.format("attribute value cannot be longer than %s characters", Constants.MAX_VALUE_LENGTH)); //$NON-NLS-1$
                    }
                    return;
                }
            }

            if (attributeValue instanceof Object[])
            {
                Object[] values = ((Object[]) attributeValue);
                List list = new ArrayList();
                for (int i = 0; i < values.length; i++)
                {
                    Object item = values[i];
                    if (null != item && !"".equals(item))
                    {
                        if ((item instanceof String) && (((String) item).getBytes().length > Constants.MAX_VALUE_LENGTH))
                        {
                            if (Constants.IS_LOGGABLE)
                            {
                                Log.e(Constants.LOG_TAG, String.format("attribute set cannot contain values longer than %s characters", Constants.MAX_VALUE_LENGTH)); //$NON-NLS-1$
                            }
                            return;
                        }
                        list.add(values[i]);
                    }
                }
                if (0 != list.size())
                {
                    JSONArray array = new JSONArray();
                    for (Object o : list)
                    {
                        array.put(o);
                    }
                    json.put(attributeName, array);
                }
                else
                {
                    return;
                }
            }
            else if (!"".equals(attributeValue.toString().trim()))
            {
                json.put(attributeName, attributeValue);
            }
            else
            {
                return;
            }

            mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_SET_PROFILE_ATTRIBUTE, new Object[]{ json, new Integer(ProfileDbAction.SET_ATTRIBUTE.ordinal()) }));
        }
        catch (JSONException e)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.w(Constants.LOG_TAG, "Caught JSON exception", e); //$NON-NLS-1$
            }
        }
    }

    /**
     * Initiates an upload of any Localytics data for this session's API key. This should be done early in the process life in
     * order to guarantee as much time as possible for slow connections to complete. It is necessary to do this even if the user
     * has opted out because this is how the opt out is transported to the webservice.
     */
    public void upload()
    {
        uploadAnalytics();
        uploadProfile();
    }

    /**
     * Internal call to begin an analytics upload.
     */
    protected void uploadAnalytics()
    {
        mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_UPLOAD, null));
    }
    
    /**
     * Internal call to begin a profile upload.
     */
    protected void uploadProfile()
    {
        mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_UPLOAD_PROFILE, null));
    }

    /*
     * This is useful, but not necessarily needed for the public API. If so desired, someone can uncomment this out.
     */
    // /**
    // * Initiates an upload of any Localytics data for this session's API key. This should be done early in the process life in
    // * order to guarantee as much time as possible for slow connections to complete. It is necessary to do this even if the user
    // * has opted out because this is how the opt out is transported to the webservice.
    // *
    // * @param callback a Runnable to execute when the upload completes. A typical use case would be to notify the caller that
    // the
    // * upload has completed. This runnable will be executed on an undefined thread, so the caller should anticipate
    // * this runnable NOT executing on the main thread or the thread that calls {@link #upload}. This parameter may be
    // * null.
    // */
    // public void upload(final Runnable callback)
    // {
    // mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_UPLOAD, callback));
    // }

    /**
     * Sorts an int value into a set of regular intervals as defined by the minimum, maximum, and step size. Both the min and max
     * values are inclusive, and in the instance where (max - min + 1) is not evenly divisible by step size, the method guarantees
     * only the minimum and the step size to be accurate to specification, with the new maximum will be moved to the next regular
     * step.
     *
     * @param actualValue The int value to be sorted.
     * @param minValue The int value representing the inclusive minimum interval.
     * @param maxValue The int value representing the inclusive maximum interval.
     * @param step The int value representing the increment of each interval.
     * @return a ranged attribute suitable for passing as the argument to {@link #tagEvent(String)} or
     *         {@link #tagEvent(String, Map)}.
     */
    public static String createRangedAttribute(final int actualValue, final int minValue, final int maxValue, final int step)
    {
        // Confirm there is at least one bucket
        if (step < 1)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.v(Constants.LOG_TAG, "Step must not be less than zero.  Returning null."); //$NON-NLS-1$
            }
            return null;
        }
        if (minValue >= maxValue)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.v(Constants.LOG_TAG, "maxValue must not be less than minValue.  Returning null."); //$NON-NLS-1$
            }
            return null;
        }

        // Determine the number of steps, rounding up using int math
        final int stepQuantity = (maxValue - minValue + step) / step;
        final int[] steps = new int[stepQuantity + 1];
        for (int currentStep = 0; currentStep <= stepQuantity; currentStep++)
        {
            steps[currentStep] = minValue + (currentStep) * step;
        }
        return createRangedAttribute(actualValue, steps);
    }

    /**
     * Sorts an int value into a predefined, pre-sorted set of intervals, returning a string representing the new expected value.
     * The array must be sorted in ascending order, with the first element representing the inclusive lower bound and the last
     * element representing the exclusive upper bound. For instance, the array [0,1,3,10] will provide the following buckets: less
     * than 0, 0, 1-2, 3-9, 10 or greater.
     *
     * @param actualValue The int value to be bucketed.
     * @param steps The sorted int array representing the bucketing intervals.
     * @return String representation of {@code actualValue} that has been bucketed into the range provided by {@code steps}.
     * @throws IllegalArgumentException if {@code steps} is null.
     * @throws IllegalArgumentException if {@code steps} has length 0.
     */
    public static String createRangedAttribute(final int actualValue, final int[] steps)
    {
        if (null == steps)
        {
            throw new IllegalArgumentException("steps cannot be null"); //$NON-NLS-1$
        }

        if (steps.length == 0)
        {
            throw new IllegalArgumentException("steps length must be greater than 0"); //$NON-NLS-1$
        }

        String bucket = null;

        // if less than smallest value
        if (actualValue < steps[0])
        {
            bucket = "less than " + steps[0];
        }
        // if greater than largest value
        else if (actualValue >= steps[steps.length - 1])
        {
            bucket = steps[steps.length - 1] + " and above";
        }
        else
        {
            // binarySearch returns the index of the value, or (-(insertion point) - 1) if not found
            int bucketIndex = Arrays.binarySearch(steps, actualValue);
            if (bucketIndex < 0)
            {
                // if the index wasn't found, then we want the value before the insertion point as the lower end
                // the special case where the insertion point is 0 is covered above, so we don't have to worry about it here
                bucketIndex = (-bucketIndex) - 2;
            }
            if (steps[bucketIndex] == (steps[bucketIndex + 1] - 1))
            {
                bucket = Integer.toString(steps[bucketIndex]);
            }
            else
            {
                bucket = steps[bucketIndex] + "-" + (steps[bucketIndex + 1] - 1); //$NON-NLS-1$
            }
        }
        return bucket;
    }

    /**
     * Helper to convert a list of dimensions into a set of attributes.
     * <p>
     * The number of dimensions is capped at 4. If there are more than 4 elements in {@code customDimensions}, all elements after
     * 4 are ignored.
     *
     * @param customDimensions List of dimensions to convert.
     * @return Attributes map for the set of dimensions.
     */
    private static Map<String, String> convertDimensionsToAttributes(final List<String> customDimensions)
    {
        final TreeMap<String, String> attributes = new TreeMap<String, String>();

        if (null != customDimensions)
        {
            int index = 0;
            for (final String element : customDimensions)
            {
                if (0 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1, element);
                }
                else if (1 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2, element);
                }
                else if (2 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3, element);
                }
                else if (3 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4, element);
                }
                else if (4 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5, element);
                }
                else if (5 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6, element);
                }
                else if (6 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7, element);
                }
                else if (7 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8, element);
                }
                else if (8 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9, element);
                }
                else if (9 == index)
                {
                    attributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10, element);
                }

                index++;
            }
        }

        return attributes;
    }

    /**
     * Enable/Disable logging.
     *
     * @param enabled Flag to indicate whether the logging should be enabled or disabled.
     */
    public static void setLoggingEnabled(boolean enabled)
    {
        Constants.IS_LOGGABLE = enabled;
    }

    /**
     * Check whether the logging is enabled or disabled
     *
     * @return true if the logging is enabled, otherwise false.
     */
    public static boolean isLoggingEnabled()
    {
        return Constants.IS_LOGGABLE;
    }

    /**
     * Enable/Disable using HTTPS to upload session data.
     *
     * @param enabled Flag to indicate whether HTTPS is enabled or not.
     */
    public static void setHttpsEnabled(boolean enabled)
    {
        Constants.USE_HTTPS = enabled;
    }

    /**
     * Check whether HTTPS is enabled or disabled for session data uploading
     *
     * @return true if HTTPS is enabled, otherwise false.
     */
    public static boolean isUsingHttps()
    {
        return Constants.USE_HTTPS;
    }

    /**
     * Set the maximum duration time allowed for session expiration.
     *
     * @param expiration The expiration time in milliseconds
     */
    public static void setSessionExpiration(long expiration)
    {
        Constants.SESSION_EXPIRATION = expiration;
    }

    /**
     * Get the maximum duration for session expiration.
     *
     * @return The session expiration time in milliseconds
     */
    public static long getSessionExpiration()
    {
        return Constants.SESSION_EXPIRATION;
    }

    /**
     * Get the analytics upload url.
     *
     * @return The analytics upload url
     */
    public static String getAnalyticsURL()
    {
        return Constants.ANALYTICS_URL;
    }

    /**
     * Set the analytics upload url.
     *
     * @param url URL to use for analytics uploads
     */
    public static void setAnalyticsURL(String url)
    {
        Constants.ANALYTICS_URL = url;
    }

    /**
     * Get the profiles upload url.
     *
     * @return The profiles upload url
     */
    public static String getProfilesURL()
    {
        return Constants.PROFILES_URL;
    }

    /**
     * Set the profiles upload url.
     *
     * @param url URL to use for profiles uploads
     */
    public static void setProfilesURL(String url)
    {
        Constants.PROFILES_URL = url;
    }
}
