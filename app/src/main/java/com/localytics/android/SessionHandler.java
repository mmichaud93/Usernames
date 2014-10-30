package com.localytics.android;

import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.database.CursorJoiner;
import android.location.Location;
import android.os.Build;
import android.os.Build.VERSION;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.Message;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;
import android.util.Pair;

import com.localytics.android.LocalyticsProvider.ApiKeysDbColumns;
import com.localytics.android.LocalyticsProvider.AttributesDbColumns;
import com.localytics.android.LocalyticsProvider.CustomDimensionsDbColumns;
import com.localytics.android.LocalyticsProvider.EventHistoryDbColumns;
import com.localytics.android.LocalyticsProvider.EventsDbColumns;
import com.localytics.android.LocalyticsProvider.IdentifiersDbColumns;
import com.localytics.android.LocalyticsProvider.InfoDbColumns;
import com.localytics.android.LocalyticsProvider.SessionsDbColumns;
import com.localytics.android.LocalyticsProvider.UploadBlobEventsDbColumns;
import com.localytics.android.LocalyticsProvider.UploadBlobsDbColumns;
import com.localytics.android.LocalyticsProvider.ProfileDbColumns;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Helper class to handle session-related work on the {@link LocalyticsSession#sSessionHandlerThread}.
 */
/* package */class SessionHandler extends Handler
{
    /**
     * Empty handler message to initialize the callback.
     * <p>
     * This message must be sent before any other messages.
     */
    public static final int MESSAGE_INIT = 0;

    /**
     * Handler message to open a session.
     * <p>
     * {@link Message#obj} is either null or a {@code Map<String, String>} containing attributes for the open.
     */
    public static final int MESSAGE_OPEN = 1;

    /**
     * Handler message to close a session.
     * <p>
     * {@link Message#obj} is either null or a {@code Map<String, String>} containing attributes for the close.
     */
    public static final int MESSAGE_CLOSE = 2;

    /**
     * Handler message to tag an event.
     * <p>
     * {@link Message#obj} is a {@link Pair} instance. This object cannot be null.
     */
    public static final int MESSAGE_TAG_EVENT = 3;

    /**
     * Handler message to upload all data collected so far
     * <p>
     * {@link Message#obj} is a {@code Runnable} to execute when upload is complete. The thread that this runnable will
     * executed on is undefined.
     */
    public static final int MESSAGE_UPLOAD = 4;

    /**
     * Empty Handler message indicating that a previously requested upload attempt was completed. This does not mean the
     * attempt was successful. A callback occurs regardless of whether upload succeeded.
     */
    public static final int MESSAGE_UPLOAD_CALLBACK = 5;

    /**
     * Handler message indicating an opt-out choice.
     * <p>
     * {@link Message#arg1} == 1 for true (opt out). 0 means opt-in.
     */
    public static final int MESSAGE_OPT_OUT = 6;

    /**
     * Handler message indicating a tag screen event
     * <p>
     * {@link Message#obj} is a string representing the screen visited.
     */
    public static final int MESSAGE_TAG_SCREEN = 7;
    
    /**
     * Handler message indicating a set identifier action
     * <p>
     * {@link Message#obj} is a string representing the screen visited.
     */
    public static final int MESSAGE_SET_IDENTIFIER = 8;

    /**
     * Handler message to register with GCM
     * <p>
     * {@link Message#obj} is a string representing the sender id.
     */
    public static final int MESSAGE_REGISTER_PUSH = 9;

    /**
     * Handler message to set the GCM registration id
     * <p>
     * {@link Message#obj} is a string representing the push registration id.
     */
    public static final int MESSAGE_SET_PUSH_REGID = 10;
    
    /**
     * Handler message to set the user location
     * <p>
     * {@link Message#obj} is a object representing the android Location.
     */
    public static final int MESSAGE_SET_LOCATION = 11;

    /**
     * Handler message to set a custom dimension
     * <p>
     * {@link Message#obj} is a list of Object containing the dimension num and the dimension value.
     */
    public static final int MESSAGE_SET_CUSTOM_DIMENSION = 12;
    
    /**
     * Handler message to trigger amp
     * <p>
     * {@link Message#obj} is a list of Object containing the event and attributes to from which to trigger the amp.
     */
    public static final int MESSAGE_AMP_TRIGGER = 13;

    /**
     * Handler message indicating an disable/enable push choice.
     * <p>
     * {@link Message#arg1} == 1 for true (disable). 0 means enable.
     */
    public static final int MESSAGE_DISABLE_PUSH = 14;

    /**
     * Handler message to show test button when test mode is enabled
     */
    public static final int MESSAGE_SHOW_AMP_TEST = 15;

    /**
     * Handler message to show modify customer profile data
     * <p>
     * {@link Message#obj} is a ordered pair containing a JSON string of the attribute and the action to perform on the profile DB
     */
    public static final int MESSAGE_SET_PROFILE_ATTRIBUTE = 16;

    /**
     * Handler message to upload all profile data collected so far
     * <p>
     * {@link Message#obj} is a {@code Runnable} to execute when upload is complete. The thread that this runnable will
     * executed on is undefined.
     */
    public static final int MESSAGE_UPLOAD_PROFILE = 17;

    /**
     * Empty Handler message indicating that a previously requested profile upload attempt was completed. This does not mean the
     * attempt was successful. A callback occurs regardless of whether upload succeeded.
     */
    public static final int MESSAGE_UPLOAD_PROFILE_CALLBACK = 18;

    /**
     * Handler message to delete all resources associated with an AMP message
     */
    public static final int MESSAGE_DELETE_AMP_RESOURCES = 19;

    /**
     * Handler message to handle registering for push notifications
     * <p>
     * {@link Message#obj} is the registration intent
     */
    public static final int MESSAGE_HANDLE_PUSH_REGISTRATION = 20;

    /**
     * Handler message to handle a push notification
     * <p>
     * {@link Message#obj} is the notification intent
     */
    public static final int MESSAGE_HANDLE_PUSH_RECEIVED = 21;

    /**
     * Sort order for the upload blobs.
     * <p>
     * This is a workaround for Android bug 3707 <http://code.google.com/p/android/issues/detail?id=3707>.
     */
    private static final String UPLOAD_BLOBS_EVENTS_SORT_ORDER = String.format("CAST(%s AS TEXT)", UploadBlobEventsDbColumns.EVENTS_KEY_REF); //$NON-NLS-1$

    /**
     * Sort order for the events.
     * <p>
     * This is a workaround for Android bug 3707 <http://code.google.com/p/android/issues/detail?id=3707>.
     */
    private static final String EVENTS_SORT_ORDER = String.format("CAST(%s as TEXT)", EventsDbColumns._ID); //$NON-NLS-1$

    /**
     * Keeps track of which Localytics clients are currently uploading, in order to allow only one upload for a given key at a
     * time.
     * <p>
     * This field can only be read/written to from the {@link LocalyticsSession#sSessionHandlerThread}. This invariant is maintained by only
     * accessing this field from within the {@link LocalyticsSession#mSessionHandler}.
     */
    protected static final Map<String, Boolean> sIsUploadingMap = new HashMap<String, Boolean>();
    
    /**
     * Keeps track of which Localytics clients are currently uploading profiles, in order to allow only one upload for a given key at a
     * time.
     * <p>
     * This field can only be read/written to from the {@link LocalyticsSession#sSessionHandlerThread}. This invariant is maintained by only
     * accessing this field from within the {@link LocalyticsSession#mSessionHandler}.
     */
    protected static final Map<String, Boolean> sIsUploadingProfileMap = new HashMap<String, Boolean>();

    /**
     * Background thread used for all Localytics upload processing. This thread is shared across all instances of
     * LocalyticsSession within a process.
     */
    /*
     * By using the class name for the HandlerThread, obfuscation through Proguard is more effective: if Proguard changes the
     * class name, the thread name also changes.
     */
    protected static final HandlerThread sUploadHandlerThread = getHandlerThread(UploadHandler.class.getSimpleName());

    /**
     * Background thread used for all Localytics profile upload processing. This thread is shared across all instances of
     * LocalyticsSession within a process.
     */
    /*
    * By using the class name for the HandlerThread, obfuscation through Proguard is more effective: if Proguard changes the
    * class name, the thread name also changes.
    */
    protected static final HandlerThread sProfileUploadHandlerThread = getHandlerThread(UploadHandler.class.getSimpleName() + "_profiles");

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
     * Application context
     */
    protected final Context mContext;

    /**
     * Localytics database
     */
    protected LocalyticsProvider mProvider;

    /**
     * The Localytics API key for the session.
     */
    protected final String mApiKey;

    /**
     * {@link ApiKeysDbColumns#_ID} for the API key used by this Localytics session handler.
     */
    private long mApiKeyId;

    /**
     * The most recent location. If non-null it will be included in sessions and events
     */
    private static Location sLastLocation = null;

    /**
     * Handler object where all upload of this instance of LocalyticsSession are handed off to.
     * <p>
     * This handler runs on {@link #sUploadHandlerThread}.
     */
    private UploadHandler mUploadHandler;

    /**
     * Handler object where all upload of this instance of LocalyticsSession are handed off to.
     * <p>
     *     This handler runs on {@link #sProfileUploadHandlerThread}.
     */
    private UploadHandler mProfileUploadHandler;

    /**
     * Creates a new Handler that runs on {@code looper}.
     * <p>
     * Note: This constructor may perform disk access.
     *
     * @param context Application context. Cannot be null.
     * @param sessionHandler Parent {@link SessionHandler} object to notify when uploads are completed. Cannot be null.
     * @param apiKey Localytics API key. Cannot be null.
     * @param installId Localytics install ID.
     * @param looper to run the Handler on. Cannot be null.
     */
    protected UploadHandler createUploadHandler(final Context context, final Handler sessionHandler, final String apiKey, final String installId, final Looper looper)
    {
    	return new UploadHandler(context, this, apiKey, installId, looper);
    }
    
    /**
     * Gets a UploadHandler that runs on {@code looper}.
     * TODO:
     * @return
     */
    /* package */UploadHandler getUploadHandler()
    {
    	return mUploadHandler;
    }

    /**
     * Gets a UploadHandler that runs on {@code looper}.
     * TODO:
     * @return
     */
    /* package */UploadHandler getProfileUploadHandler()
    {
        return mProfileUploadHandler;
    }

    /**
     * Constructs a new Handler that runs on the given looper.
     *
     * @param context The context used to access resources on behalf of the app. It is recommended to use
     *            {@link Context#getApplicationContext()} to avoid the potential memory leak incurred by maintaining
     *            references to {@code Activity} instances. Cannot be null.
     * @param key The key unique for each application generated at www.localytics.com. Cannot be null or empty.
     * @param looper to run the Handler on. Cannot be null.
     * @throws IllegalArgumentException if {@code context} is null
     * @throws IllegalArgumentException if {@code key} is null or empty
     */
    public SessionHandler(final Context context, final String key, final Looper looper)
    {
        super(looper);                

        if (Constants.IS_PARAMETER_CHECKING_ENABLED)
        {
            if (null == context)
            {
                throw new IllegalArgumentException("context cannot be null"); //$NON-NLS-1$
            }
            if (TextUtils.isEmpty(key))
            {
                throw new IllegalArgumentException("key cannot be null or empty"); //$NON-NLS-1$
            }
        }
        
        mContext = context;
        mApiKey = key;
    }

    @Override
    public void handleMessage(final Message msg)
    {
        try
        {
            super.handleMessage(msg);

            if (Constants.IS_LOGGABLE)
            {
                Log.v(Constants.LOG_TAG, String.format("Handler received %s", msg)); //$NON-NLS-1$
            }

            switch (msg.what)
            {
                case MESSAGE_INIT:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_INIT"); //$NON-NLS-1$
                    }

                    final String referrerID = (String)msg.obj;

                    SessionHandler.this.init(referrerID);

                    break;
                }
                case MESSAGE_OPT_OUT:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_OPT_OUT"); //$NON-NLS-1$
                    }

                    final boolean isOptingOut = msg.arg1 == 0 ? false : true;

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            SessionHandler.this.optOut(isOptingOut);
                        }
                    });

                    break;
                }
                case MESSAGE_OPEN:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_OPEN"); //$NON-NLS-1$
                    }

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        @SuppressWarnings("unchecked")
                        public void run()
                        {
                            SessionHandler.this.open(false, (Map<String, String>) msg.obj);
                        }
                    });

                    break;
                }
                case MESSAGE_CLOSE:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_CLOSE"); //$NON-NLS-1$
                    }

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        @SuppressWarnings("unchecked")
                        public void run()
                        {
                            SessionHandler.this.close((Map<String, String>) msg.obj);
                        }
                    });

                    break;
                }
                case MESSAGE_TAG_EVENT:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_TAG_EVENT"); //$NON-NLS-1$
                    }

                    final Object[] params = (Object[]) msg.obj;

                    final String event = (String) params[0];
                    @SuppressWarnings("unchecked")
					final Map<String, String> attributes = (Map<String, String>) params[1];
                    final Long clv = (Long) params[2];

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            if (null != getOpenSessionId(mProvider))
                            {
                                tagEvent(event, attributes, clv);
                            }
                            else
                            {
                                /*
                                 * The open and close only care about custom dimensions
                                 */
                                final Map<String, String> openCloseAttributes;
                                if (null == attributes)
                                {
                                    openCloseAttributes = null;
                                }
                                else if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9)
                                        || attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10))
                                {
                                    openCloseAttributes = new TreeMap<String, String>();
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9));
                                    }
                                    if (attributes.containsKey(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10))
                                    {
                                        openCloseAttributes.put(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10, attributes.get(AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10));
                                    }
                                }
                                else
                                {
                                    openCloseAttributes = null;
                                }

                                open(false, openCloseAttributes);
                                tagEvent(event, attributes, clv);
                                close(openCloseAttributes);
                            }
                        }
                    });

                    break;
                }
                case MESSAGE_TAG_SCREEN:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_TAG_SCREEN"); //$NON-NLS-1$
                    }

                    final String screen = (String) msg.obj;

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            SessionHandler.this.tagScreen(screen);
                        }
                    });

                    break;
                }
                case MESSAGE_SET_IDENTIFIER:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_SET_IDENTIFIER"); //$NON-NLS-1$
                    }

                    final Object[] params = (Object[]) msg.obj;
                    final String key = (String) params[0];
                    final String value = (String) params[1];
                    
                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            SessionHandler.this.setIdentifier(key, value);
                        }
                    });

                    break;
                }
                case MESSAGE_REGISTER_PUSH:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_REGISTER_PUSH"); //$NON-NLS-1$
                    }

                    final String newSenderId = (String) msg.obj;
                    
                    mProvider.runBatchTransaction(new Runnable()
                    {
                    	public void run()
                    	{
                            if (isPushDisabled())
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.d(Constants.LOG_TAG, "Push has been disabled"); //$NON-NLS-1$
                                }

                                return;
                            }

	                        Cursor cursor = null;

                            String senderId = null;
	                        String pushRegId = null;
	                        String pushRegVersion = null;
	                        try
	                        {
	                            cursor = mProvider.query(InfoDbColumns.TABLE_NAME, null, null, null, null); //$NON-NLS-1$
	
	                            if (cursor.moveToFirst())
	                            {
                                    senderId = cursor.getString(cursor.getColumnIndexOrThrow(InfoDbColumns.SENDER_ID));
	                            	pushRegVersion = cursor.getString(cursor.getColumnIndexOrThrow(InfoDbColumns.REGISTRATION_VERSION));
	                            	pushRegId = cursor.getString(cursor.getColumnIndexOrThrow(InfoDbColumns.REGISTRATION_ID));
	                            }
	                        }
	                        finally
	                        {
	                            if (null != cursor)
	                            {
	                                cursor.close();
	                                cursor = null;
	                            }
	                        }

                            // Clear registration id if user passes in a new sender id
                            if (!newSenderId.equals(senderId))
                            {
                                pushRegId = null;
                                // Save the new sender id
                                final ContentValues values = new ContentValues();
                                values.put(InfoDbColumns.SENDER_ID, newSenderId);
                                values.put(InfoDbColumns.REGISTRATION_ID, pushRegId);
                                mProvider.update(InfoDbColumns.TABLE_NAME, values, null, null);
                            }

	                        final String appVersion = DatapointHelper.getAppVersion(mContext);
	                        		                        
	                        // Only register if we don't have a registration id or if the app version has changed
	                        if (TextUtils.isEmpty(pushRegId) || !appVersion.equals(pushRegVersion))
	                        {
		                        Intent registrationIntent = new Intent("com.google.android.c2dm.intent.REGISTER");
		                        registrationIntent.putExtra("app", PendingIntent.getBroadcast(mContext, 0, new Intent(), 0));
		                        registrationIntent.putExtra("sender", newSenderId);
		                        mContext.startService(registrationIntent);
	                        }
                    	}
                    });
                    
                    break;
                }
                case MESSAGE_DISABLE_PUSH:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_DISABLE_PUSH"); //$NON-NLS-1$
                    }

                    final int disabled = msg.arg1;

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            SessionHandler.this.setPushDisabled(disabled);
                        }
                    });

                    break;
                }
                case MESSAGE_SET_PUSH_REGID:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_SET_PUSH_REGID"); //$NON-NLS-1$
                    }

                    final String pushRegId = (String) msg.obj;
                    
                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            SessionHandler.this.setPushRegistrationId(pushRegId);
                        }
                    });

                    break;
                } 
                case MESSAGE_SET_LOCATION:
                {
                	if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_SET_LOCATION"); //$NON-NLS-1$
                    }

                    sLastLocation = (Location) msg.obj;
                    
                	break;
                }
                case MESSAGE_SET_CUSTOM_DIMENSION:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_SET_CUSTOM_DIMENSION"); //$NON-NLS-1$
                    }

                    final Object[] params = (Object[]) msg.obj;
                    final int dimension = (Integer) params[0];
                    final String value = (String) params[1];

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            SessionHandler.this.setCustomDimension(dimension, value);
                        }
                    });

                    break;
                }
                case MESSAGE_UPLOAD:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "SessionHandler received MESSAGE_UPLOAD"); //$NON-NLS-1$
                    }

                    /*
                     * Note that callback may be null
                     */
                    final Runnable callback = (Runnable) msg.obj;

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            SessionHandler.this.upload(callback);
                        }
                    });

                    break;
                }
                case MESSAGE_UPLOAD_CALLBACK:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_UPLOAD_CALLBACK"); //$NON-NLS-1$
                    }

                    sIsUploadingMap.put(mApiKey, Boolean.FALSE);

                    break;
                }
                case MESSAGE_SET_PROFILE_ATTRIBUTE:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_SET_PROFILE_ATTRIBUTE"); //$NON-NLS-1$
                    }

                    final Object[] params = (Object[]) msg.obj;
                    final JSONObject attributeJSON = (JSONObject) params[0];
                    final int action = ((Integer)params[1]).intValue();
                    final String customerID = mProvider.getUserIdAndType().get(LocalyticsProvider.USER_ID);

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            // Save the attribute value
                            final ContentValues values = new ContentValues();
                            values.put(ProfileDbColumns.ATTRIBUTE, attributeJSON.toString());
                            values.put(ProfileDbColumns.ACTION, action);
                            values.put(ProfileDbColumns.CUSTOMER_ID, customerID);
                            mProvider.insert(ProfileDbColumns.TABLE_NAME, values);
                        }
                    });

                    break;
                }
                case MESSAGE_UPLOAD_PROFILE:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "SessionHandler received MESSAGE_UPLOAD_PROFILE"); //$NON-NLS-1$
                    }

                    /*
                     * Note that callback may be null
                     */
                    final Runnable callback = (Runnable) msg.obj;

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        public void run()
                        {
                            SessionHandler.this.uploadProfile(callback);
                        }
                    });

                    break;
                }
                case MESSAGE_UPLOAD_PROFILE_CALLBACK:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_UPLOAD_PROFILE_CALLBACK"); //$NON-NLS-1$
                    }

                    sIsUploadingProfileMap.put(mApiKey, Boolean.FALSE);

                    break;
                }
                case MESSAGE_HANDLE_PUSH_REGISTRATION:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_HANDLE_PUSH_REGISTRATION"); //$NON-NLS-1$
                    }

                    final Intent intent = (Intent)msg.obj;

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            String registrationId = intent.getStringExtra("registration_id");
                            // Disabled?
                            if (isPushDisabled())
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.v(Constants.LOG_TAG, "GCM registered but push disabled: removing id"); //$NON-NLS-1$
                                }

                                setPushRegistrationId(null);
                            }
                            // Failed?
                            else if (intent.getStringExtra("error") != null)
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.v(Constants.LOG_TAG, "GCM registration failed"); //$NON-NLS-1$
                                }
                            }
                            // Unregistered?
                            else if (intent.getStringExtra("unregistered") != null)
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.v(Constants.LOG_TAG, "GCM unregistered: removing id"); //$NON-NLS-1$
                                }

                                setPushRegistrationId(null);
                            }
                            // Success
                            else if (registrationId != null)
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.v(Constants.LOG_TAG, String.format("GCM registered, new id: %s", registrationId)); //$NON-NLS-1$
                                }

                                setPushRegistrationId(registrationId);
                            }
                        }
                    });

                    break;
                }
                case MESSAGE_HANDLE_PUSH_RECEIVED:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Handler received MESSAGE_HANDLE_PUSH_RECEIVED"); //$NON-NLS-1$
                    }

                    final Intent intent = (Intent)msg.obj;

                    mProvider.runBatchTransaction(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            // Check whether push is disabled
                            if (isPushDisabled())
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.w(Constants.LOG_TAG, "Got push notification while push is disabled."); //$NON-NLS-1$
                                }
                                return;
                            }

                            // Ignore messages that aren't from Localytics
                            String llString = intent.getExtras().getString("ll");
                            if (llString == null)
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.w(Constants.LOG_TAG, "Ignoring message that aren't from Localytics."); //$NON-NLS-1$
                                }
                                return;
                            }

                            // Try to parse the campaign id from the payload
                            int campaignId = 0;

                            try
                            {
                                JSONObject llObject = new JSONObject(llString);
                                campaignId = llObject.getInt("ca");
                            }
                            catch (JSONException e)
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.w(Constants.LOG_TAG, "Failed to get campaign id from payload, ignoring message"); //$NON-NLS-1$
                                }
                                return;
                            }

                            // Get the notification message
                            String message = intent.getExtras().getString("message");

                            // Get the app name, icon, and launch intent
                            CharSequence appName = "";
                            int appIcon = android.R.drawable.sym_def_app_icon;
                            Intent launchIntent = null;
                            try
                            {
                                ApplicationInfo applicationInfo = mContext.getPackageManager().getApplicationInfo(mContext.getPackageName(), 0);
                                appIcon = applicationInfo.icon;
                                appName = mContext.getPackageManager().getApplicationLabel(applicationInfo);
                                launchIntent = mContext.getPackageManager().getLaunchIntentForPackage(mContext.getPackageName());
                            }
                            catch (PackageManager.NameNotFoundException e)
                            {
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.w(Constants.LOG_TAG, "Failed to get application name, icon, or launch intent"); //$NON-NLS-1$
                                }
                            }

                            // Create the notification
                            Notification notification = new Notification(appIcon, message, System.currentTimeMillis());

                            // Set the intent to perform when tapped
                            if (launchIntent != null)
                            {
                                launchIntent.putExtras(intent);
                                PendingIntent contentIntent = PendingIntent.getActivity(mContext, 0, launchIntent, PendingIntent.FLAG_UPDATE_CURRENT);
                                notification.setLatestEventInfo(mContext, appName, message, contentIntent);
                            }

                            // Auto dismiss when tapped
                            notification.flags |= Notification.FLAG_AUTO_CANCEL;

                            // Show the notification (use the campaign id as the notification id to prevents duplicates)
                            NotificationManager notificationManager = (NotificationManager) mContext.getSystemService(
                                    Context.NOTIFICATION_SERVICE);
                            notificationManager.notify(campaignId, notification);
                        }
                    });

                    break;
                }
                default:
                {
                    /*
                     * This should never happen
                     */
                    throw new RuntimeException("Fell through switch statement"); //$NON-NLS-1$
                }
            }
        }
        catch (final Exception e)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.e(Constants.LOG_TAG, "Localytics library threw an uncaught exception", e); //$NON-NLS-1$
            }

            if (!Constants.IS_EXCEPTION_SUPPRESSION_ENABLED)
            {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Projection for querying details of the current API key
     */
    private static final String[] PROJECTION_INIT_API_KEY = new String[]
        {
            ApiKeysDbColumns._ID,
            ApiKeysDbColumns.OPT_OUT,
            ApiKeysDbColumns.CREATED_TIME,
            ApiKeysDbColumns.UUID };

    /**
     * Selection for a specific API key ID
     */
    private static final String SELECTION_INIT_API_KEY = String.format("%s = ?", ApiKeysDbColumns.API_KEY); //$NON-NLS-1$

    /**
     * Initialize the handler post construction.
     * <p>
     * This method must only be called once.
     * <p>
     * Note: This method is a private implementation detail. It is only made package accessible for unit testing purposes. The
     * public interface is to send {@link #MESSAGE_INIT} to the Handler.
     *
     * @see #MESSAGE_INIT
     */
    /* package */void init(final String referrerID)
    {
        mProvider = LocalyticsProvider.getInstance(mContext, mApiKey);

        Cursor cursor = null;
        try
        {
            cursor = mProvider.query(ApiKeysDbColumns.TABLE_NAME, PROJECTION_INIT_API_KEY, SELECTION_INIT_API_KEY, new String[]
                { mApiKey }, null);

            if (cursor.moveToFirst())
            {
                // API key was previously created
                if (Constants.IS_LOGGABLE)
                {
                    Log.v(Constants.LOG_TAG, String.format("Loading details for API key %s", mApiKey)); //$NON-NLS-1$
                }

                mApiKeyId = cursor.getLong(cursor.getColumnIndexOrThrow(ApiKeysDbColumns._ID));

                if (cursor.getLong(cursor.getColumnIndexOrThrow(ApiKeysDbColumns.CREATED_TIME)) == 0)
                {
                    final ContentValues values = new ContentValues();
                    values.put(ApiKeysDbColumns.CREATED_TIME, Long.valueOf(System.currentTimeMillis()));
                    mProvider.update(ApiKeysDbColumns.TABLE_NAME, values, SELECTION_INIT_API_KEY, new String[]{ mApiKey });
                }
            }
            else
            {
                // perform first-time initialization of API key
                if (Constants.IS_LOGGABLE)
                {
                    Log.v(Constants.LOG_TAG, String.format("Performing first-time initialization for new API key %s", mApiKey)); //$NON-NLS-1$
                }

                final ContentValues values = new ContentValues();
                values.put(ApiKeysDbColumns.API_KEY, mApiKey);
                values.put(ApiKeysDbColumns.UUID, UUID.randomUUID().toString());
                values.put(ApiKeysDbColumns.OPT_OUT, Boolean.FALSE);
                values.put(ApiKeysDbColumns.CREATED_TIME, Long.valueOf(null == referrerID ? System.currentTimeMillis() : 0));

                mApiKeyId = mProvider.insert(ApiKeysDbColumns.TABLE_NAME, values);
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }

        try
        {
            cursor = mProvider.query(InfoDbColumns.TABLE_NAME, new String[]{InfoDbColumns.PLAY_ATTRIBUTION}, null, null, null);

            if (cursor.moveToFirst())
            {
                String currentReferrerID = cursor.getString(cursor.getColumnIndexOrThrow(InfoDbColumns.PLAY_ATTRIBUTION));
                if (null == currentReferrerID && null != referrerID && !TextUtils.isEmpty(referrerID))
                {
                    final ContentValues values = new ContentValues();
                    values.put(InfoDbColumns.PLAY_ATTRIBUTION, referrerID);
                    mProvider.update(InfoDbColumns.TABLE_NAME, values, null, null);
                }
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }

        if (!sIsUploadingMap.containsKey(mApiKey))
        {
            sIsUploadingMap.put(mApiKey, Boolean.FALSE);
        }

        if (!sIsUploadingProfileMap.containsKey(mApiKey))
        {
            sIsUploadingProfileMap.put(mApiKey, Boolean.FALSE);
        }

        /*
         * Perform lazy initialization of the UploadHandler
         */
        String installationID = getInstallationId(mProvider, mApiKey);
        mUploadHandler = createUploadHandler(mContext, this, mApiKey, installationID, sUploadHandlerThread.getLooper());
        mProfileUploadHandler = createUploadHandler(mContext, this, mApiKey, installationID, sProfileUploadHandlerThread.getLooper());
    }

    /**
     * Selection for {@link #optOut(boolean)}.
     */
    private static final String SELECTION_OPT_IN_OUT = String.format("%s = ?", ApiKeysDbColumns._ID); //$NON-NLS-1$

    /**
     * Set the opt-in/out-out state for all sessions using the current API key.
     * <p>
     * This method must only be called after {@link #init(String)} is called.
     * <p>
     * Note: This method is a private implementation detail. It is only made package accessible for unit testing purposes. The
     * public interface is to send {@link #MESSAGE_OPT_OUT} to the Handler.
     * <p>
     * If a session is already open when an opt-out request is made, then data for the remainder of that session will be
     * collected. For example, calls to {@link #tagEvent(String, Map)} and {@link #tagScreen(String)} will be recorded until
     * {@link #close(Map)} is called.
     * <p>
     * If a session is not already open when an opt-out request is made, a new session is opened and closed by this method in
     * order to cause the opt-out event to be uploaded.
     *
     * @param isOptingOut true if the user is opting out. False if the user is opting back in.
     * @see #MESSAGE_OPT_OUT
     */
	/* package */void optOut(final boolean isOptingOut)
    {
        if (Constants.IS_LOGGABLE)
        {
            Log.v(Constants.LOG_TAG, String.format("Requested opt-out state is %b", Boolean.valueOf(isOptingOut))); //$NON-NLS-1$
        }

        // Do nothing if opt-out is unchanged
        if (isOptedOut(mProvider, mApiKey) == isOptingOut)
        {
            return;
        }

        if (null == getOpenSessionId(mProvider))
        {
            /*
             * Force a session to contain the opt event
             */
            open(true, null);
            tagEvent(isOptingOut ? Constants.OPT_OUT_EVENT : Constants.OPT_IN_EVENT, null);
            close(null);
        }
        else
        {
            tagEvent(isOptingOut ? Constants.OPT_OUT_EVENT : Constants.OPT_IN_EVENT, null);
        }

        final ContentValues values = new ContentValues();
        values.put(ApiKeysDbColumns.OPT_OUT, Boolean.valueOf(isOptingOut));
        mProvider.update(ApiKeysDbColumns.TABLE_NAME, values, SELECTION_OPT_IN_OUT, new String[]
            { Long.toString(mApiKeyId) });
    }

    /**
     * Projection for {@link #getOpenSessionId(LocalyticsProvider)}.
     */
    private static final String[] PROJECTION_GET_OPEN_SESSION_ID_SESSION_ID = new String[]
        { SessionsDbColumns._ID };

    /**
     * Projection for getting the event count in {@link #getOpenSessionId(LocalyticsProvider)}.
     */
    private static final String[] PROJECTION_GET_OPEN_SESSION_ID_EVENT_COUNT = new String[]
        { EventsDbColumns._COUNT };

    /**
     * Selection for {@link #getOpenSessionId(LocalyticsProvider)}.
     */
    private static final String SELECTION_GET_OPEN_SESSION_ID_EVENT_COUNT = String.format("%s = ? AND %s = ?", EventsDbColumns.SESSION_KEY_REF, EventsDbColumns.EVENT_NAME);

    /**
     * @param provider The database to query. Cannot be null.
     * @return The {@link SessionsDbColumns#_ID} of the currently open session or {@code null} if no session is open. The
     *         definition of "open" is whether a session has been opened without a corresponding close event.
     */
    /* package */static Long getOpenSessionId(final LocalyticsProvider provider)
    {
        /*
         * Get the ID of the last session
         */
        final Long sessionId;
        {
            Cursor sessionsCursor = null;
            try
            {

                /*
                 * Query all sessions sorted by session ID, which guarantees to obtain the last session regardless of whether
                 * the system clock changed.
                 */
                sessionsCursor = provider.query(SessionsDbColumns.TABLE_NAME, PROJECTION_GET_OPEN_SESSION_ID_SESSION_ID, null, null, SessionsDbColumns._ID);

                if (sessionsCursor.moveToLast())
                {
                    sessionId = Long.valueOf(sessionsCursor.getLong(sessionsCursor.getColumnIndexOrThrow(SessionsDbColumns._ID)));
                }
                else
                {
                    return null;
                }
            }
            finally
            {
                if (null != sessionsCursor)
                {
                    sessionsCursor.close();
                    sessionsCursor = null;
                }
            }
        }

        /*
         * See if the session has a close event.
         */
        Cursor eventsCursor = null;
        try
        {
            eventsCursor = provider.query(EventsDbColumns.TABLE_NAME, PROJECTION_GET_OPEN_SESSION_ID_EVENT_COUNT, SELECTION_GET_OPEN_SESSION_ID_EVENT_COUNT, new String[]
                {
                    sessionId.toString(),
                    Constants.CLOSE_EVENT }, null);

            if (eventsCursor.moveToFirst())
            {
                if (0 == eventsCursor.getInt(0))
                {
                    return sessionId;
                }
            }
        }
        finally
        {
            if (null != eventsCursor)
            {
                eventsCursor.close();
                eventsCursor = null;
            }
        }

        return null;
    }

    /**
     * Projection for {@link #open(boolean, Map)}.
     */
    private static final String[] PROJECTION_OPEN_EVENT_ID = new String[]
        { EventsDbColumns._ID };

    /**
     * Selection for {@link #open(boolean, Map)}.
     */
    private static final String SELECTION_OPEN = String.format("%s = ? AND %s >= ?", EventsDbColumns.EVENT_NAME, EventsDbColumns.WALL_TIME); //$NON-NLS-1$

    /**
     * Projection for {@link #open(boolean, Map)}.
     */
    private static final String[] PROJECTION_OPEN_BLOB_EVENTS = new String[]
        { UploadBlobEventsDbColumns.EVENTS_KEY_REF };

    /**
     * Projection for {@link #open(boolean, Map)}.
     */
    private static final String[] PROJECTION_OPEN_SESSIONS = new String[]
        {
            SessionsDbColumns._ID,
            SessionsDbColumns.SESSION_START_WALL_TIME };

    /**
     * Selection for {@link #openNewSession(Map)}.
     */
    private static final String SELECTION_OPEN_NEW_SESSION = String.format("%s = ?", ApiKeysDbColumns.API_KEY); //$NON-NLS-1$

    /**
     * Selection for {@link #open(boolean, Map)}.
     */
    private static final String SELECTION_OPEN_DELETE_EMPTIES_EVENT_HISTORY_SESSION_KEY_REF = String.format("%s = ?", EventHistoryDbColumns.SESSION_KEY_REF); //$NON-NLS-1$

    /**
     * Selection for {@link #open(boolean, Map)}.
     */
    private static final String SELECTION_OPEN_DELETE_EMPTIES_EVENTS_SESSION_KEY_REF = String.format("%s = ?", EventsDbColumns.SESSION_KEY_REF); //$NON-NLS-1$

    /**
     * Projection for {@link #open(boolean, Map)}.
     */
    private static final String[] PROJECTION_OPEN_DELETE_EMPTIES_EVENT_ID = new String[]
        { EventsDbColumns._ID };

    /**
     * Projection for {@link #open(boolean, Map)}.
     */
    private static final String[] PROJECTION_OPEN_DELETE_EMPTIES_PROCESSED_IN_BLOB = new String[]
        { EventHistoryDbColumns.PROCESSED_IN_BLOB };

    /**
     * Selection for {@link #open(boolean, Map)}.
     */
    private static final String SELECTION_OPEN_DELETE_EMPTIES_UPLOAD_BLOBS_ID = String.format("%s = ?", UploadBlobsDbColumns._ID); //$NON-NLS-1$

    /**
     * Selection for {@link #open(boolean, Map)}.
     */
    private static final String SELECTION_OPEN_DELETE_EMPTIES_SESSIONS_ID = String.format("%s = ?", SessionsDbColumns._ID); //$NON-NLS-1$

    /**
     * Open a session. While this method should only be called once without an intervening call to {@link #close(Map)},
     * nothing bad will happen if it is called multiple times.
     * <p>
     * This method must only be called after {@link #init(String)} is called.
     * <p>
     * Note: This method is a private implementation detail. It is only made package accessible for unit testing purposes. The
     * public interface is to send {@link #MESSAGE_OPEN} to the Handler.
     *
     * @param ignoreLimits true to ignore limits on the number of sessions. False to enforce limits.
     * @param attributes Attributes to attach to the open. May be null indicating no attributes. Cannot contain null or empty
     *            keys or values.
     * @see #MESSAGE_OPEN
     */
    /* package */void open(final boolean ignoreLimits, final Map<String, String> attributes)
    {
    	if (null != getOpenSessionId(mProvider))
    	{
    		if (Constants.IS_LOGGABLE)
    		{
    			Log.w(Constants.LOG_TAG, "Session was already open"); //$NON-NLS-1$
    		}

    		return;
    	}

        if (isOptedOut(mProvider, mApiKey))
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.d(Constants.LOG_TAG, "Data collection is opted out"); //$NON-NLS-1$
            }
            return;
        }

        /*
         * There are two cases: 1. New session and 2. Re-connect to old session. There are two ways to reconnect to an old
         * session. One is by the age of the close event, and the other is by the age of the open event.
         */

        long closeEventId = -1; // sentinel value

        {
            Cursor eventsCursor = null;
            Cursor blob_eventsCursor = null;
            try
            {
                eventsCursor = mProvider.query(EventsDbColumns.TABLE_NAME, PROJECTION_OPEN_EVENT_ID, SELECTION_OPEN, new String[]
                    {
                		Constants.CLOSE_EVENT,
                        Long.toString(System.currentTimeMillis() - Constants.SESSION_EXPIRATION) }, EVENTS_SORT_ORDER);
                blob_eventsCursor = mProvider.query(UploadBlobEventsDbColumns.TABLE_NAME, PROJECTION_OPEN_BLOB_EVENTS, null, null, UPLOAD_BLOBS_EVENTS_SORT_ORDER);

                final int idColumn = eventsCursor.getColumnIndexOrThrow(EventsDbColumns._ID);
                final CursorJoiner joiner = new CursorJoiner(eventsCursor, PROJECTION_OPEN_EVENT_ID, blob_eventsCursor, PROJECTION_OPEN_BLOB_EVENTS);

                for (final CursorJoiner.Result joinerResult : joiner)
                {
                    switch (joinerResult)
                    {
                        case LEFT:
                        {

                            if (-1 != closeEventId)
                            {
                                /*
                                 * This should never happen
                                 */
                                if (Constants.IS_LOGGABLE)
                                {
                                    Log.w(Constants.LOG_TAG, "There were multiple close events within SESSION_EXPIRATION"); //$NON-NLS-1$
                                }

                                final long newClose = eventsCursor.getLong(eventsCursor.getColumnIndexOrThrow(EventsDbColumns._ID));
                                if (newClose > closeEventId)
                                {
                                    closeEventId = newClose;
                                }
                            }

                            if (-1 == closeEventId)
                            {
                                closeEventId = eventsCursor.getLong(idColumn);
                            }

                            break;
                        }
                        case BOTH:
                            break;
                        case RIGHT:
                            break;
                    }
                }
                /*
                 * Verify that the session hasn't already been flagged for upload. That could happen if
                 */
            }
            finally
            {
                if (null != eventsCursor)
                {
                    eventsCursor.close();
                    eventsCursor = null;
                }
                if (null != blob_eventsCursor)
                {
                    blob_eventsCursor.close();
                    blob_eventsCursor = null;
                }
            }
        }

        if (-1 != closeEventId)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.v(Constants.LOG_TAG, "Opening old closed session and reconnecting"); //$NON-NLS-1$
            }

            openClosedSession(closeEventId);
        }
        else
        {
            Cursor sessionsCursor = null;
            try
            {
                sessionsCursor = mProvider.query(SessionsDbColumns.TABLE_NAME, PROJECTION_OPEN_SESSIONS, null, null, SessionsDbColumns._ID);

                if (sessionsCursor.moveToLast())
                {
                    if (sessionsCursor.getLong(sessionsCursor.getColumnIndexOrThrow(SessionsDbColumns.SESSION_START_WALL_TIME)) >= System.currentTimeMillis()
                            - Constants.SESSION_EXPIRATION)
                    {
                        // reconnect
                        if (Constants.IS_LOGGABLE)
                        {
                            Log.v(Constants.LOG_TAG, "Opening old unclosed session and reconnecting"); //$NON-NLS-1$
                        }
                        return;
                    }

                    // delete empties
                    Cursor eventsCursor = null;
                    try
                    {
                        final String sessionId = Long.toString(sessionsCursor.getLong(sessionsCursor.getColumnIndexOrThrow(SessionsDbColumns._ID)));
                        final String[] sessionIdSelection = new String[]
                            { sessionId };
                        eventsCursor = mProvider.query(EventsDbColumns.TABLE_NAME, PROJECTION_OPEN_DELETE_EMPTIES_EVENT_ID, SELECTION_OPEN_DELETE_EMPTIES_EVENTS_SESSION_KEY_REF, sessionIdSelection, null);

                        if (eventsCursor.getCount() == 0)
                        {
                            final List<Long> blobsToDelete = new LinkedList<Long>();

                            // delete all event history and the upload blob
                            Cursor eventHistory = null;
                            try
                            {
                                eventHistory = mProvider.query(EventHistoryDbColumns.TABLE_NAME, PROJECTION_OPEN_DELETE_EMPTIES_PROCESSED_IN_BLOB, SELECTION_OPEN_DELETE_EMPTIES_EVENT_HISTORY_SESSION_KEY_REF, sessionIdSelection, null);
                                while (eventHistory.moveToNext())
                                {
                                    blobsToDelete.add(Long.valueOf(eventHistory.getLong(eventHistory.getColumnIndexOrThrow(EventHistoryDbColumns.PROCESSED_IN_BLOB))));
                                }
                            }
                            finally
                            {
                                if (null != eventHistory)
                                {
                                    eventHistory.close();
                                    eventHistory = null;
                                }
                            }

                            mProvider.remove(EventHistoryDbColumns.TABLE_NAME, SELECTION_OPEN_DELETE_EMPTIES_EVENT_HISTORY_SESSION_KEY_REF, sessionIdSelection);
                            for (final long blobId : blobsToDelete)
                            {
                                mProvider.remove(UploadBlobsDbColumns.TABLE_NAME, SELECTION_OPEN_DELETE_EMPTIES_UPLOAD_BLOBS_ID, new String[]
                                    { Long.toString(blobId) });
                            }
                            // mProvider.delete(AttributesDbColumns.TABLE_NAME, String.format("%s = ?",
                            // AttributesDbColumns.EVENTS_KEY_REF), selectionArgs)
                            mProvider.remove(SessionsDbColumns.TABLE_NAME, SELECTION_OPEN_DELETE_EMPTIES_SESSIONS_ID, sessionIdSelection);
                        }
                    }
                    finally
                    {
                        if (null != eventsCursor)
                        {
                            eventsCursor.close();
                            eventsCursor = null;
                        }
                    }
                }
            }
            finally
            {
                if (null != sessionsCursor)
                {
                    sessionsCursor.close();
                    sessionsCursor = null;
                }
            }

            /*
             * Check that the maximum number of sessions hasn't been exceeded
             */
            if (!ignoreLimits && getNumberOfSessions(mProvider) >= Constants.MAX_NUM_SESSIONS)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.w(Constants.LOG_TAG, "Maximum number of sessions are already on disk--not writing any new sessions until old sessions are cleared out.  Try calling upload() to store more sessions."); //$NON-NLS-1$
                }
            }
            else
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.v(Constants.LOG_TAG, "Opening new session"); //$NON-NLS-1$
                }

                openNewSession(attributes);
            }
        }
    }

    /**
     * Opens a new session. This is a helper method to {@link #open(boolean, Map)}.
     *
     * @effects Updates the database by creating a new entry in the {@link SessionsDbColumns} table.
     * @param attributes Attributes to attach to the session. May be null. Cannot contain null or empty keys or values.
     */
    private void openNewSession(final Map<String, String> attributes)
    {
        final TelephonyManager telephonyManager = (TelephonyManager) mContext.getSystemService(Context.TELEPHONY_SERVICE);

        Cursor cursor = null;
        long sessionStartTime = System.currentTimeMillis();
        long lastSessionStartTime = 0L;
        try
        {
            cursor = mProvider.query(InfoDbColumns.TABLE_NAME, null, null, null, null);

            if (cursor.moveToFirst())
            {
                lastSessionStartTime = cursor.getLong(cursor.getColumnIndexOrThrow(InfoDbColumns.LAST_SESSION_OPEN_TIME));
            }
        }
        finally
        {
            if (lastSessionStartTime == 0L)
            {
                lastSessionStartTime = sessionStartTime;
            }

            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }
        final ContentValues values = new ContentValues();
        values.put(SessionsDbColumns.API_KEY_REF, Long.valueOf(mApiKeyId));
        values.put(SessionsDbColumns.SESSION_START_WALL_TIME, Long.valueOf(sessionStartTime));
        values.put(SessionsDbColumns.UUID, UUID.randomUUID().toString());
        values.put(SessionsDbColumns.APP_VERSION, DatapointHelper.getAppVersion(mContext));
        values.put(SessionsDbColumns.ANDROID_SDK, Integer.valueOf(Constants.CURRENT_API_LEVEL));
        values.put(SessionsDbColumns.ANDROID_VERSION, VERSION.RELEASE);

        // Try and get the deviceId. If it is unavailable (or invalid) use the installation ID instead.
        String deviceId = DatapointHelper.getAndroidIdHashOrNull(mContext);
        if (null == deviceId)
        {
            deviceId = "";
        }

        values.put(SessionsDbColumns.DEVICE_ANDROID_ID_HASH, deviceId);
        values.put(SessionsDbColumns.DEVICE_ANDROID_ID, DatapointHelper.getAndroidIdOrNull(mContext));
        values.put(SessionsDbColumns.DEVICE_ADVERTISING_ID, DatapointHelper.getAdvertisingIdOrNull(mContext));
        values.put(SessionsDbColumns.DEVICE_COUNTRY, telephonyManager.getSimCountryIso());
        values.put(SessionsDbColumns.DEVICE_MANUFACTURER, DatapointHelper.getManufacturer());
        values.put(SessionsDbColumns.DEVICE_MODEL, Build.MODEL);
        values.put(SessionsDbColumns.DEVICE_SERIAL_NUMBER_HASH, DatapointHelper.getSerialNumberHashOrNull());
        values.put(SessionsDbColumns.DEVICE_TELEPHONY_ID, DatapointHelper.getTelephonyDeviceIdOrNull(mContext));
        values.putNull(SessionsDbColumns.DEVICE_TELEPHONY_ID_HASH);
        values.putNull(SessionsDbColumns.DEVICE_WIFI_MAC_HASH);
        values.put(SessionsDbColumns.LOCALE_COUNTRY, Locale.getDefault().getCountry());
        values.put(SessionsDbColumns.LOCALE_LANGUAGE, Locale.getDefault().getLanguage());
        values.put(SessionsDbColumns.LOCALYTICS_LIBRARY_VERSION, Constants.LOCALYTICS_CLIENT_LIBRARY_VERSION);
        values.put(SessionsDbColumns.LOCALYTICS_INSTALLATION_ID, getInstallationId(mProvider, mApiKey));

        values.putNull(SessionsDbColumns.LATITUDE);
        values.putNull(SessionsDbColumns.LONGITUDE);
        values.put(SessionsDbColumns.NETWORK_CARRIER, telephonyManager.getNetworkOperatorName());
        values.put(SessionsDbColumns.NETWORK_COUNTRY, telephonyManager.getNetworkCountryIso());
        values.put(SessionsDbColumns.NETWORK_TYPE, DatapointHelper.getNetworkType(mContext, telephonyManager));

        values.put(SessionsDbColumns.ELAPSED_TIME_SINCE_LAST_SESSION, Long.valueOf(sessionStartTime - lastSessionStartTime));

        long sessionId = mProvider.insert(SessionsDbColumns.TABLE_NAME, values);
        if (sessionId == -1)
        {
            throw new AssertionError("session insert failed"); //$NON-NLS-1$
        }

        // set last_session_open_time in the info table
        values.clear();
        values.put(InfoDbColumns.LAST_SESSION_OPEN_TIME, Long.valueOf(sessionStartTime));
        mProvider.update(InfoDbColumns.TABLE_NAME, values, null, null);

        setFirstAdvertisingId(DatapointHelper.getAdvertisingIdOrNull(mContext));

        tagEvent(Constants.OPEN_EVENT, attributes);

        /*
         * This is placed here so that the DatapointHelper has a chance to retrieve the old UUID before it is deleted.
         */
        LocalyticsProvider.deleteOldFiles(mContext);
    }

    /**
     * Set the first advertising id if it's not been set yet.
     *
     * @param advertisingId The advertising id got from play service.
     */
    private void setFirstAdvertisingId(final String advertisingId)
    {
        String firstAdvertisingId = null;

        Cursor cursor = null;
        try
        {
            cursor = mProvider.query(InfoDbColumns.TABLE_NAME, null, null, null, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
                firstAdvertisingId = cursor.getString(cursor.getColumnIndexOrThrow(InfoDbColumns.FIRST_ADVERTISING_ID));
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }

        if (!TextUtils.isEmpty(firstAdvertisingId))
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.v(Constants.LOG_TAG, "First advertising id has already been set before."); //$NON-NLS-1$
            }
            return;
        }

        final ContentValues values = new ContentValues();
        values.put(InfoDbColumns.FIRST_ADVERTISING_ID, advertisingId);
        mProvider.update(InfoDbColumns.TABLE_NAME, values, null, null);
    }

    /**
     * Projection for getting the installation ID. Used by {@link #getInstallationId(LocalyticsProvider, String)}.
     */
    private static final String[] PROJECTION_GET_INSTALLATION_ID = new String[]
        { ApiKeysDbColumns.UUID };

    /**
     * Selection for a specific API key ID. Used by {@link #getInstallationId(LocalyticsProvider, String)}.
     */
    private static final String SELECTION_GET_INSTALLATION_ID = String.format("%s = ?", ApiKeysDbColumns.API_KEY); //$NON-NLS-1$

    /**
     * Gets the installation ID of the API key.
     */
    /* package */ static String getInstallationId(final LocalyticsProvider provider, final String apiKey)
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(ApiKeysDbColumns.TABLE_NAME, PROJECTION_GET_INSTALLATION_ID, SELECTION_GET_INSTALLATION_ID, new String[]
                { apiKey }, null);

            if (cursor.moveToFirst())
            {
                return cursor.getString(cursor.getColumnIndexOrThrow(ApiKeysDbColumns.UUID));
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }

        /*
         * This error case shouldn't normally happen
         */
        if (Constants.IS_LOGGABLE)
        {
            Log.w(Constants.LOG_TAG, "Installation ID couldn't be found"); //$NON-NLS-1$
        }
        return null;
    }
    
    /**
     * Gets Facebook attributon cookie for an app key
     *
     * @param provider Localytics database provider. Cannot be null.
     * @return The FB attribution cookie.
     */
    /* package */static String getFBAttribution(final LocalyticsProvider provider)
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(InfoDbColumns.TABLE_NAME, null, null, null, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
                return cursor.getString(cursor.getColumnIndexOrThrow(InfoDbColumns.FB_ATTRIBUTION));
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }
        
        return null;
    }

    /**
     * Projection for {@link #openClosedSession(long)}.
     */
    private static final String[] PROJECTION_OPEN_CLOSED_SESSION = new String[]
        { EventsDbColumns.SESSION_KEY_REF };

    /**
     * Selection for {@link #openClosedSession(long)}.
     */
    private static final String SELECTION_OPEN_CLOSED_SESSION = String.format("%s = ?", EventsDbColumns._ID); //$NON-NLS-1$

    /**
     * Selection for {@link #openClosedSession(long)}.
     */
    private static final String SELECTION_OPEN_CLOSED_SESSION_ATTRIBUTES = String.format("%s = ?", AttributesDbColumns.EVENTS_KEY_REF); //$NON-NLS-1$

    /**
     * Reopens a previous session. This is a helper method to {@link #open(boolean, Map)}.
     *
     * @param closeEventId The last close event which is to be deleted so that the old session can be reopened
     * @effects Updates the database by deleting the last close event.
     */
    private void openClosedSession(final long closeEventId)
    {
        final String[] selectionArgs = new String[]
            { Long.toString(closeEventId) };

        Cursor cursor = null;
        try
        {
            cursor = mProvider.query(EventsDbColumns.TABLE_NAME, PROJECTION_OPEN_CLOSED_SESSION, SELECTION_OPEN_CLOSED_SESSION, selectionArgs, null);

            if (cursor.moveToFirst())
            {
                mProvider.remove(AttributesDbColumns.TABLE_NAME, SELECTION_OPEN_CLOSED_SESSION_ATTRIBUTES, selectionArgs);
                mProvider.remove(EventsDbColumns.TABLE_NAME, SELECTION_OPEN_CLOSED_SESSION, selectionArgs);
            }
            else
            {
                /*
                 * This should never happen
                 */

                if (Constants.IS_LOGGABLE)
                {
                    Log.e(Constants.LOG_TAG, "Event no longer exists"); //$NON-NLS-1$
                }

                openNewSession(null);
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }
    }

    /**
     * Projection for {@link #getNumberOfSessions(LocalyticsProvider)}.
     */
    private static final String[] PROJECTION_GET_NUMBER_OF_SESSIONS = new String[]
        { SessionsDbColumns._ID };

    /**
     * Helper method to get the number of sessions currently in the database.
     *
     * @param provider Instance of {@link LocalyticsProvider}. Cannot be null.
     * @return The number of sessions on disk.
     */
    /* package */static long getNumberOfSessions(final LocalyticsProvider provider)
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(SessionsDbColumns.TABLE_NAME, PROJECTION_GET_NUMBER_OF_SESSIONS, null, null, null);

            return cursor.getCount();
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }
    }

    /**
     * Close a session. While this method should only be called after {@link #open(boolean, Map)}, nothing bad will happen if
     * it is called and {@link #open(boolean, Map)} wasn't called. Similarly, nothing bad will happen if close is called
     * multiple times.
     * <p>
     * This method must only be called after {@link #init(String)} is called.
     * <p>
     * Note: This method is a private implementation detail. It is only made package accessible for unit testing purposes. The
     * public interface is to send {@link #MESSAGE_CLOSE} to the Handler.
     *
     * @param attributes Set of attributes to attach to the close. May be null indicating no attributes. Cannot contain null
     *            or empty keys or values.
     * @see #MESSAGE_OPEN
     */
    /* package */void close(final Map<String, String> attributes)
    {
        if (null == getOpenSessionId(mProvider)) // do nothing if session is not open
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.w(Constants.LOG_TAG, "Session was not open, so close is not possible."); //$NON-NLS-1$
            }
            return;
        }

        tagEvent(Constants.CLOSE_EVENT, attributes);
    }

    /**
     * Projection for {@link #tagEvent(String, Map)}.
     */
    private static final String[] PROJECTION_TAG_EVENT = new String[]
        { SessionsDbColumns.SESSION_START_WALL_TIME };

    /**
     * Selection for {@link #tagEvent(String, Map)}.
     */
    private static final String SELECTION_TAG_EVENT = String.format("%s = ?", SessionsDbColumns._ID); //$NON-NLS-1$

    /**
     * Tag an event in a session. Although this method SHOULD NOT be called unless a session is open, actually doing so will
     * have no effect.
     * <p>
     * This method must only be called after {@link #init(String)} is called.
     * <p>
     * Note: This method is a private implementation detail. It is only made package accessible for unit testing purposes. The
     * public interface is to send {@link #MESSAGE_TAG_EVENT} to the Handler.
     *
     * @param event The name of the event which occurred. Cannot be null.
     * @param attributes The collection of attributes for this particular event. May be null.
     * @see #MESSAGE_TAG_EVENT
     */
    /* package */void tagEvent(final String event, final Map<String, String> attributes)
    {
    	tagEvent(event, attributes, null);
    }
    
    /**
     * Tag an event in a session. Although this method SHOULD NOT be called unless a session is open, actually doing so will
     * have no effect.
     * <p>
     * This method must only be called after {@link #init(String)} is called.
     * <p>
     * Note: This method is a private implementation detail. It is only made package accessible for unit testing purposes. The
     * public interface is to send {@link #MESSAGE_TAG_EVENT} to the Handler.
     *
     * @param event The name of the event which occurred. Cannot be null.
     * @param attributes The collection of attributes for this particular event. May be null.
     * @param clv The customer value increase.
     * @see #MESSAGE_TAG_EVENT
     */
    /* package */void tagEvent(final String event, Map<String, String> attributes, final Long clv)
    {
        final Long openSessionId = getOpenSessionId(mProvider);
        if (null == openSessionId)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.w(Constants.LOG_TAG, "Event not written because a session is not open"); //$NON-NLS-1$
            }
            return;
        }

        /*
         * Insert the event and get the event's database ID
         */
        final long eventId;
        {
            final ContentValues values = new ContentValues();
            values.put(EventsDbColumns.SESSION_KEY_REF, openSessionId);
            values.put(EventsDbColumns.UUID, UUID.randomUUID().toString());
            values.put(EventsDbColumns.EVENT_NAME, event);
            values.put(EventsDbColumns.REAL_TIME, Long.valueOf(SystemClock.elapsedRealtime()));
            values.put(EventsDbColumns.WALL_TIME, Long.valueOf(System.currentTimeMillis()));
            
            if (null != clv)
            {
            	values.put(EventsDbColumns.CLV_INCREASE, clv);
            }
            else
            {
            	values.put(EventsDbColumns.CLV_INCREASE, 0);
            }
            
            if (sLastLocation != null)
            {
            	values.put(EventsDbColumns.LAT_NAME, sLastLocation.getLatitude());
            	values.put(EventsDbColumns.LNG_NAME, sLastLocation.getLongitude());
            }
            
            /*
             * Special case for open event: keep the start time in sync with the start time put into the sessions table.
             */
            if (Constants.OPEN_EVENT.equals(event))
            {
                Cursor cursor = null;
                try
                {
                    cursor = mProvider.query(SessionsDbColumns.TABLE_NAME, PROJECTION_TAG_EVENT, SELECTION_TAG_EVENT, new String[]
                        { openSessionId.toString() }, null);

                    if (cursor.moveToFirst())
                    {
                        values.put(EventsDbColumns.WALL_TIME, Long.valueOf(cursor.getLong(cursor.getColumnIndexOrThrow(SessionsDbColumns.SESSION_START_WALL_TIME))));
                    }
                    else
                    {
                        // this should never happen
                        throw new AssertionError("During tag of open event, session didn't exist"); //$NON-NLS-1$
                    }
                }
                finally
                {
                    if (null != cursor)
                    {
                        cursor.close();
                        cursor = null;
                    }
                }
            }

            Map id = mProvider.getUserIdAndType();
            final String customerID = (String)id.get(LocalyticsProvider.USER_ID);
            final String userType = (String)id.get(LocalyticsProvider.USER_TYPE);
            values.put(EventsDbColumns.CUST_ID, customerID);
            values.put(EventsDbColumns.USER_TYPE, userType);
            try
            {
                JSONObject identifiers = UploadHandler.getIdentifiers(mProvider);
                if (null != identifiers)
                {
                    values.put(EventsDbColumns.IDENTIFIERS, identifiers.toString());
                }
            }
            catch (final JSONException e)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.w(Constants.LOG_TAG, "Caught exception", e); //$NON-NLS-1$
                }
            }
            eventId = mProvider.insert(EventsDbColumns.TABLE_NAME, values);

            if (-1 == eventId)
            {
                throw new RuntimeException("Inserting event failed"); //$NON-NLS-1$
            }
        }

        /*
         * Add sticky custom dimensions if they have been set
         */
        for (int i = 0; i < Constants.MAX_CUSTOM_DIMENSIONS; ++i)
        {
            final String key   = CUSTOM_DIMENSION_KEYS[i];
            final String value = getCustomDimension(i);
            if (null != value)
            {
                if (null == attributes)
                {
                    attributes = new TreeMap<String, String>();
                }

                // Currently we don't overwrite with the sticky custom dimension
                if (attributes.get(key) == null)
                {
                    attributes.put(key, value);
                }
            }
        }

        /*
         * If attributes exist, insert them as well
         */
        if (null != attributes)
        {
            // reusable object
            final ContentValues values = new ContentValues();

            final String applicationAttributePrefix = String.format(AttributesDbColumns.ATTRIBUTE_FORMAT, mContext.getPackageName(), ""); //$NON-NLS-1$
            int applicationAttributeCount = 0;

            for (final Entry<String, String> entry : attributes.entrySet())
            {
                /*
                 * Detect excess application events
                 */
                if (entry.getKey().startsWith(applicationAttributePrefix))
                {
                    applicationAttributeCount++;
                    if (applicationAttributeCount > Constants.MAX_NUM_ATTRIBUTES)
                    {
                        continue;
                    }
                }

                values.put(AttributesDbColumns.EVENTS_KEY_REF, Long.valueOf(eventId));
                values.put(AttributesDbColumns.ATTRIBUTE_KEY, entry.getKey());
                values.put(AttributesDbColumns.ATTRIBUTE_VALUE, entry.getValue());

                final long id = mProvider.insert(AttributesDbColumns.TABLE_NAME, values);

                if (-1 == id)
                {
                    throw new AssertionError("Inserting attribute failed"); //$NON-NLS-1$
                }

                values.clear();
            }
        }

        /*
         * Insert the event into the history, only for application events
         */
        if (!Constants.OPEN_EVENT.equals(event) && !Constants.CLOSE_EVENT.equals(event) && !Constants.OPT_IN_EVENT.equals(event) && !Constants.OPT_OUT_EVENT.equals(event) && !Constants.FLOW_EVENT.equals(event))
        {
            final ContentValues values = new ContentValues();
            values.put(EventHistoryDbColumns.NAME, event.substring(mContext.getPackageName().length() + 1, event.length()));
            values.put(EventHistoryDbColumns.TYPE, Integer.valueOf(EventHistoryDbColumns.TYPE_EVENT));
            values.put(EventHistoryDbColumns.SESSION_KEY_REF, openSessionId);
            values.putNull(EventHistoryDbColumns.PROCESSED_IN_BLOB);
            mProvider.insert(EventHistoryDbColumns.TABLE_NAME, values);

            conditionallyAddFlowEvent();
        }
    }

    /**
     * Projection for {@link #tagScreen(String)}.
     */
    private static final String[] PROJECTION_TAG_SCREEN = new String[]
        { EventHistoryDbColumns.NAME };

    /**
     * Selection for {@link #tagScreen(String)}.
     */
    private static final String SELECTION_TAG_SCREEN = String.format("%s = ? AND %s = ?", EventHistoryDbColumns.TYPE, EventHistoryDbColumns.SESSION_KEY_REF); //$NON-NLS-1$

    /**
     * Sort order for {@link #tagScreen(String)}.
     */
    private static final String SORT_ORDER_TAG_SCREEN = String.format("%s DESC", EventHistoryDbColumns._ID); //$NON-NLS-1$

    /**
     * Tag a screen in a session. While this method shouldn't be called unless {@link #open(boolean, Map)} is called first,
     * this method will simply do nothing if {@link #open(boolean, Map)} hasn't been called.
     * <p>
     * This method performs duplicate suppression, preventing multiple screens with the same value in a row within a given
     * session.
     * <p>
     * This method must only be called after {@link #init(String)} is called.
     * <p>
     * Note: This method is a private implementation detail. It is only made public for unit testing purposes. The public
     * interface is to send {@link #MESSAGE_TAG_SCREEN} to the Handler.
     *
     * @param screen The name of the screen which occurred. Cannot be null or empty.
     * @see #MESSAGE_TAG_SCREEN
     */
    /* package */void tagScreen(final String screen)
    {
        final Long openSessionId = getOpenSessionId(mProvider);
        if (null == openSessionId)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.w(Constants.LOG_TAG, "Tag not written because the session was not open"); //$NON-NLS-1$
            }
            return;
        }

        /*
         * Do duplicate suppression
         */
        Cursor cursor = null;
        try
        {
            cursor = mProvider.query(EventHistoryDbColumns.TABLE_NAME, PROJECTION_TAG_SCREEN, SELECTION_TAG_SCREEN, new String[]
                {
                    Integer.toString(EventHistoryDbColumns.TYPE_SCREEN),
                    openSessionId.toString() }, SORT_ORDER_TAG_SCREEN);

            if (cursor.moveToFirst())
            {
                if (screen.equals(cursor.getString(cursor.getColumnIndexOrThrow(EventHistoryDbColumns.NAME))))
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.v(Constants.LOG_TAG, String.format("Suppressed duplicate screen %s", screen)); //$NON-NLS-1$
                    }
                    return;
                }
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }

        /*
         * Write the screen to the database
         */
        final ContentValues values = new ContentValues();
        values.put(EventHistoryDbColumns.NAME, screen);
        values.put(EventHistoryDbColumns.TYPE, Integer.valueOf(EventHistoryDbColumns.TYPE_SCREEN));
        values.put(EventHistoryDbColumns.SESSION_KEY_REF, openSessionId);
        values.putNull(EventHistoryDbColumns.PROCESSED_IN_BLOB);
        mProvider.insert(EventHistoryDbColumns.TABLE_NAME, values);

        conditionallyAddFlowEvent();
    }
    
    /**
     * Projection for {@link #setIdentifier(String, String)}.
     */
    private static final String[] PROJECTION_SET_IDENTIFIER = new String[] { IdentifiersDbColumns.KEY, IdentifiersDbColumns.VALUE };

    /**
     * Selection for {@link #setIdentifier(String, String)}.
     */
    private static final String SELECTION_SET_IDENTIFIER = String.format("%s = ?", IdentifiersDbColumns.KEY); //$NON-NLS-1$

    /* package */void setIdentifier(final String key, final String value)
    {
        Cursor cursor = null;
        try
        {
            cursor = mProvider.query(IdentifiersDbColumns.TABLE_NAME, PROJECTION_SET_IDENTIFIER, SELECTION_SET_IDENTIFIER, new String[] { key }, null);

            if (cursor.moveToFirst())
            {
            	if (null == value)
            	{
            		mProvider.remove(IdentifiersDbColumns.TABLE_NAME, String.format("%s = ?", IdentifiersDbColumns.KEY), new String[] { cursor.getString(cursor.getColumnIndexOrThrow(IdentifiersDbColumns.KEY)) }); //$NON-NLS-1$
            	}
            	else
            	{
                    String currentValue = cursor.getString(cursor.getColumnIndexOrThrow(IdentifiersDbColumns.VALUE));
                    if (!value.equals(currentValue))
                    {
                        final ContentValues values = new ContentValues();
                        values.put(IdentifiersDbColumns.KEY, key);
                        values.put(IdentifiersDbColumns.VALUE, value);
                        mProvider.update(IdentifiersDbColumns.TABLE_NAME, values, SELECTION_SET_IDENTIFIER, new String[]{key});
                    }
            	}
            }
            else
            {
            	if (value != null)
            	{
            		final ContentValues values = new ContentValues();
            		values.put(IdentifiersDbColumns.KEY, key);
            		values.put(IdentifiersDbColumns.VALUE, value);                       
            		mProvider.insert(IdentifiersDbColumns.TABLE_NAME, values);
            	}
            }

        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }
    }

    /**
     * Projection for {@link #setCustomDimension(int, String)}.
     */
    private static final String[] PROJECTION_SET_CUSTOM_DIMENSION = new String[] { CustomDimensionsDbColumns.CUSTOM_DIMENSION_VALUE };

    /**
     * Selection for {@link #setCustomDimension(int, String)}.
     */
    private static final String SELECTION_SET_CUSTOM_DIMENSION = String.format("%s = ?", CustomDimensionsDbColumns.CUSTOM_DIMENSION_KEY); //$NON-NLS-1$

    /**
     * The table of custom dimension keys.
     */
    private static final String[] CUSTOM_DIMENSION_KEYS = new String[] { CustomDimensionsDbColumns.CUSTOM_DIMENSION_1, CustomDimensionsDbColumns.CUSTOM_DIMENSION_2, CustomDimensionsDbColumns.CUSTOM_DIMENSION_3, CustomDimensionsDbColumns.CUSTOM_DIMENSION_4, CustomDimensionsDbColumns.CUSTOM_DIMENSION_5, CustomDimensionsDbColumns.CUSTOM_DIMENSION_6, CustomDimensionsDbColumns.CUSTOM_DIMENSION_7, CustomDimensionsDbColumns.CUSTOM_DIMENSION_8, CustomDimensionsDbColumns.CUSTOM_DIMENSION_9, CustomDimensionsDbColumns.CUSTOM_DIMENSION_10 };

    /**
     * Set the custom dimension of the given dimension with a new value.
     * If the custom dimension doesn't exist, a new one will be created.
     *
     * @param dimension The dimension to set.
     * @param value The value to set. If the value is null, the given dimension will be deleted.
     */
    /* package */void setCustomDimension(int dimension, final String value)
    {
        // Also add the check here because setCustomDimension is also invoked from Javascript.
        if (dimension < 0 || dimension > 9)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.w(Constants.LOG_TAG, "Only valid dimensions are 0 - 9");
            }
            return;
        }

        final String key = CUSTOM_DIMENSION_KEYS[dimension];

        mProvider.runBatchTransaction(new Runnable()
        {
            public void run()
            {
                Cursor cursor = null;
                try
                {
                    cursor = mProvider.query(CustomDimensionsDbColumns.TABLE_NAME, PROJECTION_SET_CUSTOM_DIMENSION, SELECTION_SET_CUSTOM_DIMENSION, new String[] { key }, null);

                    if (cursor.moveToFirst())
                    {
                        if (null == value)
                        {
                            mProvider.remove(CustomDimensionsDbColumns.TABLE_NAME, String.format("%s = ?", CustomDimensionsDbColumns.CUSTOM_DIMENSION_KEY), new String[] { key }); //$NON-NLS-1$
                        }
                        else
                        {
                            final ContentValues values = new ContentValues();
                            values.put(CustomDimensionsDbColumns.CUSTOM_DIMENSION_KEY, key);
                            values.put(CustomDimensionsDbColumns.CUSTOM_DIMENSION_VALUE, value);
                            mProvider.update(CustomDimensionsDbColumns.TABLE_NAME, values, SELECTION_SET_CUSTOM_DIMENSION, new String[] { key });
                        }
                    }
                    else
                    {
                        if (value != null)
                        {
                            final ContentValues values = new ContentValues();
                            values.put(CustomDimensionsDbColumns.CUSTOM_DIMENSION_KEY, key);
                            values.put(CustomDimensionsDbColumns.CUSTOM_DIMENSION_VALUE, value);
                            mProvider.insert(CustomDimensionsDbColumns.TABLE_NAME, values);
                        }
                    }

                }
                finally
                {
                    if (null != cursor)
                    {
                        cursor.close();
                        cursor = null;
                    }
                }
            }
        });

    }

    /**
     * Get the custom dimension of the given dimension.
     *
     * @param dimension The dimension from which to get the custom dimension.
     * @return the value of the custom dimension of the given dimension.
     */
    /* package */String getCustomDimension(int dimension)
    {
        if (dimension < 0 || dimension > 9)
        {
            return null;
        }

        String value = null;
        final String key = CUSTOM_DIMENSION_KEYS[dimension];

        Cursor cursor = null;
        try
        {
            cursor = mProvider.query(CustomDimensionsDbColumns.TABLE_NAME, PROJECTION_SET_CUSTOM_DIMENSION, SELECTION_SET_CUSTOM_DIMENSION, new String[] { key }, null);

            if (cursor.moveToFirst())
            {
                value = cursor.getString(cursor.getColumnIndexOrThrow(CustomDimensionsDbColumns.CUSTOM_DIMENSION_VALUE));
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }

        return value;
    }

    /* package */void setPushRegistrationId(final String pushRegId)
    {
        final ContentValues values = new ContentValues();
        values.put(InfoDbColumns.REGISTRATION_ID, pushRegId == null ? "" : pushRegId);
        values.put(InfoDbColumns.REGISTRATION_VERSION, DatapointHelper.getAppVersion(mContext));
        mProvider.update(InfoDbColumns.TABLE_NAME, values, null, null);
    }

    private boolean isPushDisabled()
    {
        boolean disabled = false;
        Cursor cursor = null;
        try
        {
            cursor = mProvider.query(InfoDbColumns.TABLE_NAME, new String[]{InfoDbColumns.PUSH_DISABLED}, null, null, null);
            while (cursor.moveToNext())
            {
                int disabledBit = cursor.getInt(cursor.getColumnIndexOrThrow(InfoDbColumns.PUSH_DISABLED));
                disabled = disabledBit == 1;
            }
        }
        finally
        {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
            return disabled;
        }
    }

    /* package */void setPushDisabled(final int disabled)
    {
        final ContentValues values = new ContentValues();
        values.put(InfoDbColumns.PUSH_DISABLED, disabled);
        mProvider.update(InfoDbColumns.TABLE_NAME, values, null, null);
    }

    /**
     * Projection for {@link #conditionallyAddFlowEvent()}.
     */
    private static final String[] PROJECTION_FLOW_EVENTS = new String[]
        { EventsDbColumns._ID };

    /**
     * Selection for {@link #conditionallyAddFlowEvent()}.
     */
    private static final String SELECTION_FLOW_EVENTS = String.format("%s = ?", EventsDbColumns.EVENT_NAME); //$NON-NLS-1$

    /**
     * Selection arguments for {@link #SELECTION_FLOW_EVENTS} in {@link #conditionallyAddFlowEvent()}.
     */
    private static final String[] SELECTION_ARGS_FLOW_EVENTS = new String[]
        { Constants.FLOW_EVENT };

    /**
     * Projection for {@link #conditionallyAddFlowEvent()}.
     */
    private static final String[] PROJECTION_FLOW_BLOBS = new String[]
        { UploadBlobEventsDbColumns.EVENTS_KEY_REF };

    /**
     * Conditionally adds a flow event if no flow event exists in the current upload blob.
     */
    private void conditionallyAddFlowEvent()
    {
        /*
         * Creating a flow "event" is required to act as a placeholder so that the uploader will know that an upload needs to
         * occur. A flow event should only be created if there isn't already a flow event that hasn't been associated with an
         * upload blob.
         */
        boolean foundUnassociatedFlowEvent = false;

        Cursor eventsCursor = null;
        Cursor blob_eventsCursor = null;
        try
        {
            eventsCursor = mProvider.query(EventsDbColumns.TABLE_NAME, PROJECTION_FLOW_EVENTS, SELECTION_FLOW_EVENTS, SELECTION_ARGS_FLOW_EVENTS, EVENTS_SORT_ORDER);

            blob_eventsCursor = mProvider.query(UploadBlobEventsDbColumns.TABLE_NAME, PROJECTION_FLOW_BLOBS, null, null, UPLOAD_BLOBS_EVENTS_SORT_ORDER);

            final CursorJoiner joiner = new CursorJoiner(eventsCursor, PROJECTION_FLOW_EVENTS, blob_eventsCursor, PROJECTION_FLOW_BLOBS);
            for (final CursorJoiner.Result joinerResult : joiner)
            {
                switch (joinerResult)
                {
                    case LEFT:
                    {
                        foundUnassociatedFlowEvent = true;
                        break;
                    }
                    case BOTH:
                        break;
                    case RIGHT:
                        break;
                }
            }
        }
        finally
        {
            if (null != eventsCursor)
            {
                eventsCursor.close();
                eventsCursor = null;
            }

            if (null != blob_eventsCursor)
            {
                blob_eventsCursor.close();
                blob_eventsCursor = null;
            }
        }

        if (!foundUnassociatedFlowEvent)
        {
            tagEvent(Constants.FLOW_EVENT, null);
        }
    }

    /**
     * Projection for {@link #preUploadBuildBlobs(LocalyticsProvider)}.
     */
    private static final String[] PROJECTION_UPLOAD_EVENTS = new String[]
        {
            EventsDbColumns._ID,
            EventsDbColumns.EVENT_NAME,
            EventsDbColumns.WALL_TIME };

    /**
     * Projection for {@link #preUploadBuildBlobs(LocalyticsProvider)}.
     */
    private static final String[] PROJECTION_UPLOAD_BLOBS = new String[]
        { UploadBlobEventsDbColumns.EVENTS_KEY_REF };

    /**
     * Projection for {@link #preUploadBuildBlobs(LocalyticsProvider)}.
     */
    private static final String SELECTION_UPLOAD_NULL_BLOBS = String.format("%s IS NULL", EventHistoryDbColumns.PROCESSED_IN_BLOB); //$NON-NLS-1$

    /**
     * Columns to join in {@link #preUploadBuildBlobs(LocalyticsProvider)}.
     */
    private static final String[] JOINER_ARG_UPLOAD_EVENTS_COLUMNS = new String[]
        { EventsDbColumns._ID };

    /**
     * Builds upload blobs for all events.
     *
     * @param provider Instance of {@link LocalyticsProvider}. Cannot be null.
     * @effects Mutates the database by creating a new upload blob for all events that are unassociated at the time this
     *          method is called.
     */
    /* package */static void preUploadBuildBlobs(final LocalyticsProvider provider)
    {
        /*
         * Group all events that aren't part of an upload blob into a new blob. While this process is a linear algorithm that
         * requires scanning two database tables, the performance won't be a problem for two reasons: 1. This process happens
         * frequently so the number of events to group will always be low. 2. There is a maximum number of events, keeping the
         * overall size low. Note that close events that are younger than SESSION_EXPIRATION will be skipped to allow session
         * reconnects.
         */

        // temporary set of event ids that aren't in a blob
        final Set<Long> eventIds = new HashSet<Long>();

        Cursor eventsCursor = null;
        Cursor blob_eventsCursor = null;
        try
        {
            eventsCursor = provider.query(EventsDbColumns.TABLE_NAME, PROJECTION_UPLOAD_EVENTS, null, null, EVENTS_SORT_ORDER);

            blob_eventsCursor = provider.query(UploadBlobEventsDbColumns.TABLE_NAME, PROJECTION_UPLOAD_BLOBS, null, null, UPLOAD_BLOBS_EVENTS_SORT_ORDER);

            final int idColumn = eventsCursor.getColumnIndexOrThrow(EventsDbColumns._ID);
            final CursorJoiner joiner = new CursorJoiner(eventsCursor, JOINER_ARG_UPLOAD_EVENTS_COLUMNS, blob_eventsCursor, PROJECTION_UPLOAD_BLOBS);
            for (final CursorJoiner.Result joinerResult : joiner)
            {
                switch (joinerResult)
                {
                    case LEFT:
                    {
                        if (Constants.CLOSE_EVENT.equals(eventsCursor.getString(eventsCursor.getColumnIndexOrThrow(EventsDbColumns.EVENT_NAME))))
                        {
                            if (System.currentTimeMillis() - eventsCursor.getLong(eventsCursor.getColumnIndexOrThrow(EventsDbColumns.WALL_TIME)) < Constants.SESSION_EXPIRATION)
                            {
                                break;
                            }
                        }
                        eventIds.add(Long.valueOf(eventsCursor.getLong(idColumn)));
                        break;
                    }
                    case BOTH:
                        break;
                    case RIGHT:
                        break;
                }
            }
        }
        finally
        {
            if (null != eventsCursor)
            {
                eventsCursor.close();
                eventsCursor = null;
            }

            if (null != blob_eventsCursor)
            {
                blob_eventsCursor.close();
                blob_eventsCursor = null;
            }
        }

        if (eventIds.size() > 0)
        {
            // reusable object
            final ContentValues values = new ContentValues();

            final Long blobId;
            {
                values.put(UploadBlobsDbColumns.UUID, UUID.randomUUID().toString());
                blobId = Long.valueOf(provider.insert(UploadBlobsDbColumns.TABLE_NAME, values));
                values.clear();
            }

            for (final Long x : eventIds)
            {
                values.put(UploadBlobEventsDbColumns.UPLOAD_BLOBS_KEY_REF, blobId);
                values.put(UploadBlobEventsDbColumns.EVENTS_KEY_REF, x);

                provider.insert(UploadBlobEventsDbColumns.TABLE_NAME, values);

                values.clear();
            }

            values.put(EventHistoryDbColumns.PROCESSED_IN_BLOB, blobId);
            provider.update(EventHistoryDbColumns.TABLE_NAME, values, SELECTION_UPLOAD_NULL_BLOBS, null);
            values.clear();
        }
    }

    /**
     * Initiate upload of all session data currently stored on disk.
     * <p>
     * This method must only be called after {@link #init(String)} is called. The session does not need to be open for an upload to
     * occur.
     * <p>
     * Note: This method is a private implementation detail. It is only made package accessible for unit testing purposes. The
     * public interface is to send {@link #MESSAGE_UPLOAD} to the Handler.
     *
     * @param callback An optional callback to perform once the upload completes. May be null for no callback.
     * @see #MESSAGE_UPLOAD
     */
    /* package */void upload(final Runnable callback)
    {
        if (sIsUploadingMap.get(mApiKey).booleanValue())
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.d(Constants.LOG_TAG, "Already uploading"); //$NON-NLS-1$
            }

            mUploadHandler.sendMessage(mUploadHandler.obtainMessage(UploadHandler.MESSAGE_RETRY_UPLOAD_REQUEST, callback));
            return;
        }

        try
        {
            preUploadBuildBlobs(mProvider);

            sIsUploadingMap.put(mApiKey, Boolean.TRUE);
            mUploadHandler.sendMessage(mUploadHandler.obtainMessage(UploadHandler.MESSAGE_UPLOAD, callback));
        }
        catch (final Exception e)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.w(Constants.LOG_TAG, "Error occurred during upload", e); //$NON-NLS-1$
            }

            sIsUploadingMap.put(mApiKey, Boolean.FALSE);

            // Notify the caller the upload is "complete"
            if (null != callback)
            {
                /*
                 * Note that a new thread is created for the callback. This ensures that client code can't affect the
                 * performance of the SessionHandler's thread.
                 */
                new Thread(callback, UploadHandler.UPLOAD_CALLBACK_THREAD_NAME).start();
            }
        }
    }

    /**
     * Initiate upload of all profile data currently stored on disk.
     * <p>
     * This method must only be called after {@link #init(String)} is called. The session does not need to be open for an upload to
     * occur.
     * <p>
     * Note: This method is a private implementation detail. It is only made package accessible for unit testing purposes. The
     * public interface is to send {@link #MESSAGE_UPLOAD_PROFILE} to the Handler.
     *
     * @param callback An optional callback to perform once the upload completes. May be null for no callback.
     * @see #MESSAGE_UPLOAD_PROFILE
     */
    /* package */void uploadProfile(final Runnable callback)
    {
        if (sIsUploadingProfileMap.get(mApiKey).booleanValue())
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.d(Constants.LOG_TAG, "Already uploading profile"); //$NON-NLS-1$
            }

            mProfileUploadHandler.sendMessage(mProfileUploadHandler.obtainMessage(UploadHandler.MESSAGE_RETRY_UPLOAD_PROFILE_REQUEST, callback));
            return;
        }

        try
        {
            sIsUploadingProfileMap.put(mApiKey, Boolean.TRUE);
            mProfileUploadHandler.sendMessage(mProfileUploadHandler.obtainMessage(UploadHandler.MESSAGE_UPLOAD_PROFILE, callback));
        }
        catch (final Exception e)
        {
            if (Constants.IS_LOGGABLE)
            {
                Log.w(Constants.LOG_TAG, "Error occurred during profile upload", e); //$NON-NLS-1$
            }

            sIsUploadingProfileMap.put(mApiKey, Boolean.FALSE);

            // Notify the caller the upload is "complete"
            if (null != callback)
            {
                /*
                 * Note that a new thread is created for the callback. This ensures that client code can't affect the
                 * performance of the SessionHandler's thread.
                 */
                new Thread(callback, UploadHandler.UPLOAD_PROFILE_CALLBACK_THREAD_NAME).start();
            }
        }
    }

    /**
     * Projection for {@link #isOptedOut(LocalyticsProvider, String)}.
     */
    private static final String[] PROJECTION_IS_OPTED_OUT = new String[]
        { ApiKeysDbColumns.OPT_OUT };

    /**
     * Selection for {@link #isOptedOut(LocalyticsProvider, String)}.
     * <p>
     * The selection argument is the {@link ApiKeysDbColumns#API_KEY}.
     */
    private static final String SELECTION_IS_OPTED_OUT = String.format("%s = ?", ApiKeysDbColumns.API_KEY); //$NON-NLS-1$

    /**
     * @param provider Instance of {@link LocalyticsProvider}. Cannot be null.
     * @param apiKey Api key to test whether it is opted out. Cannot be null.
     * @return true if data collection has been opted out. Returns false if data collection is opted-in or if {@code apiKey}
     *         doesn't exist in the database.
     * @throws IllegalArgumentException if {@code provider} is null.
     * @throws IllegalArgumentException if {@code apiKey} is null.
     */
    /* package */static boolean isOptedOut(final LocalyticsProvider provider, final String apiKey)
    {
        if (Constants.IS_PARAMETER_CHECKING_ENABLED)
        {
            if (null == provider)
            {
                throw new IllegalArgumentException("provider cannot be null"); //$NON-NLS-1$
            }

            if (null == apiKey)
            {
                throw new IllegalArgumentException("apiKey cannot be null"); //$NON-NLS-1$
            }
        }

        Cursor cursor = null;
        try
        {
            cursor = provider.query(ApiKeysDbColumns.TABLE_NAME, PROJECTION_IS_OPTED_OUT, SELECTION_IS_OPTED_OUT, new String[]
                { apiKey }, null);

            if (cursor.moveToFirst())
            {
                return cursor.getInt(cursor.getColumnIndexOrThrow(ApiKeysDbColumns.OPT_OUT)) != 0;
            }
        }
        finally
        {
            if (null != cursor)
            {
                cursor.close();
                cursor = null;
            }
        }

        return false;
    }    
}    


