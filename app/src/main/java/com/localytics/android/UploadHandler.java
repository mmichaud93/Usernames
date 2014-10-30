package com.localytics.android;

import android.content.Context;
import android.database.Cursor;
import android.os.Handler;
import android.os.Looper;
import android.os.Message;
import android.text.TextUtils;
import android.text.format.DateUtils;
import android.util.Log;

import com.localytics.android.JsonObjects.BlobHeader;
import com.localytics.android.LocalyticsProvider.ApiKeysDbColumns;
import com.localytics.android.LocalyticsProvider.AttributesDbColumns;
import com.localytics.android.LocalyticsProvider.EventHistoryDbColumns;
import com.localytics.android.LocalyticsProvider.EventsDbColumns;
import com.localytics.android.LocalyticsProvider.IdentifiersDbColumns;
import com.localytics.android.LocalyticsProvider.InfoDbColumns;
import com.localytics.android.LocalyticsProvider.ProfileDbColumns;
import com.localytics.android.LocalyticsProvider.SessionsDbColumns;
import com.localytics.android.LocalyticsProvider.UploadBlobEventsDbColumns;
import com.localytics.android.LocalyticsProvider.UploadBlobsDbColumns;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

/**
 * Helper object to the {@link SessionHandler} which helps process upload requests.
 */
/* package */class UploadHandler extends Handler
{

    /**
     * Thread name that the upload callback runnable is executed on.
     */
    public static final String UPLOAD_CALLBACK_THREAD_NAME = "upload_callback"; //$NON-NLS-1$

    /**
     * Thread name that the profile upload callback runnable is executed on.
     */
    public static final String UPLOAD_PROFILE_CALLBACK_THREAD_NAME = "upload_profile_callback"; //$NON-NLS-1$

    /**
     * Localytics analytics upload URL for HTTP, as a format string that contains a format for the API key.
     */
    private final static String ANALYTICS_URL_HTTP = "http://%s/api/v2/applications/%s/uploads"; //$NON-NLS-1$

    /**
     * Localytics analytics upload URL for HTTPS
     */
    private final static String ANALYTICS_URL_HTTPS = "https://%s/api/v2/uploads"; //$NON-NLS-1$
    
    /**
     * Localytics profile upload URL
     */
    private final static String PROFILE_URL = "https://%s/v1/apps/%s/profiles/%s"; //$NON-NLS-1$

    /**
     * Handler message to upload all data collected so far
     * <p>
     * {@link Message#obj} is a {@code Runnable} to execute when upload is complete. The thread that this runnable will
     * executed on is undefined.
     */
    public static final int MESSAGE_UPLOAD = 1;

    /**
     * Handler message indicating that there is a queued upload request. When this message is processed, this handler simply
     * forwards the request back to {@link LocalyticsSession#mSessionHandler} with {@link SessionHandler#MESSAGE_UPLOAD}.
     * <p>
     * {@link Message#obj} is a {@code Runnable} to execute when upload is complete. The thread that this runnable will
     * executed on is undefined.
     */
    public static final int MESSAGE_RETRY_UPLOAD_REQUEST = 2;

    /**
     * Handler message to upload all profile data collected so far
     * <p>
     * {@link Message#obj} is a {@code Runnable} to execute when upload is complete. The thread that this runnable will
     * executed on is undefined.
     */
    public static final int MESSAGE_UPLOAD_PROFILE = 3;

    /**
     * Handler message indicating that there is a queued upload profile request. When this message is processed, this handler simply
     * forwards the request back to {@link LocalyticsSession#mSessionHandler} with {@link SessionHandler#MESSAGE_UPLOAD_PROFILE}.
     * <p>
     * {@link Message#obj} is a {@code Runnable} to execute when upload is complete. The thread that this runnable will
     * executed on is undefined.
     */
    public static final int MESSAGE_RETRY_UPLOAD_PROFILE_REQUEST = 4;

    /**
     * Reference to the Localytics database
     */
    protected final LocalyticsProvider mProvider;

    /**
     * Application context
     */
    protected final Context mContext;

    /**
     * The Localytics API key
     */
    protected final String mApiKey;
    
    /**
     * The Localytics Install ID
     */
    private final String mInstallId;
    
    /**
     * Parent session handler to notify when an upload completes.
     */
    private final Handler mSessionHandler;

    /**
     * Constructs a new Handler that runs on {@code looper}.
     * <p>
     * Note: This constructor may perform disk access.
     *
     * @param context Application context. Cannot be null.
     * @param sessionHandler Parent {@link SessionHandler} object to notify when uploads are completed. Cannot be null.
     * @param apiKey Localytics API key. Cannot be null.
     * @param installId Localytics install ID.
     * @param looper to run the Handler on. Cannot be null.
     */
    public UploadHandler(final Context context, final Handler sessionHandler, final String apiKey, final String installId, final Looper looper)
    {
        super(looper);

        mContext = context;
        mProvider = LocalyticsProvider.getInstance(context, apiKey);
        mSessionHandler = sessionHandler;
        mApiKey = apiKey;
        mInstallId = installId;
    }

    private String getApiKey()
    {
        String apiKey = mApiKey;
        String rollupKey = DatapointHelper.getLocalyticsRollupKeyOrNull(mContext);
        if (rollupKey != null && !TextUtils.isEmpty(rollupKey))
        {
            apiKey = rollupKey;
        }

        return apiKey;
    }

    @Override
    public void handleMessage(final Message msg)
    {
        try
        {
            super.handleMessage(msg);

            switch (msg.what)
            {
                case MESSAGE_UPLOAD:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "UploadHandler received MESSAGE_UPLOAD"); //$NON-NLS-1$
                    }

                    /*
                     * Note that callback may be null
                     */
                    final Runnable callback = (Runnable) msg.obj;

                    try
                    {                            
                        final List<JSONObject> toUpload = convertDatabaseToJson(mContext, mProvider, mApiKey);

                        if (!toUpload.isEmpty())
                        {
                            final StringBuilder builder = new StringBuilder();
                            for (final JSONObject json : toUpload)
                            {
                                builder.append(json.toString());
                                builder.append('\n');
                            }
                            
                            String apiKey = getApiKey();
                            String customerID = getCustomerID(mProvider);

                            if (upload(UploadType.SESSIONS, Constants.USE_HTTPS ? String.format(ANALYTICS_URL_HTTPS, Constants.ANALYTICS_URL) : String.format(ANALYTICS_URL_HTTP, Constants.ANALYTICS_URL, apiKey), builder.toString(), mInstallId, apiKey, customerID))
                            {
                                mProvider.runBatchTransaction(new Runnable()
                                {
                                    public void run()
                                    {
                                        deleteBlobsAndSessions(mProvider);
                                    }
                                });
                            }
                        }
                    }
                    finally
                    {
                        if (null != callback)
                        {
                            /*
                             * Execute the callback on a separate thread, to avoid exposing this thread to the client of the
                             * library
                             */
                            new Thread(callback, UPLOAD_CALLBACK_THREAD_NAME).start();
                        }

                        mSessionHandler.sendEmptyMessage(SessionHandler.MESSAGE_UPLOAD_CALLBACK);
                    }
                    break;
                }
                case MESSAGE_RETRY_UPLOAD_REQUEST:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Received MESSAGE_RETRY_UPLOAD_REQUEST"); //$NON-NLS-1$
                    }

                    mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_UPLOAD, msg.obj));
                    break;
                }
                case MESSAGE_UPLOAD_PROFILE:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "UploadHandler received MESSAGE_UPLOAD_PROFILE"); //$NON-NLS-1$
                    }

                    /*
                     * Note that callback may be null
                     */
                    final Runnable callback = (Runnable) msg.obj;

                    String apiKey = getApiKey();
                    // set attributes as ready to be uploaded
                    TreeMap<String, ProfileUploadDataPair> profileAttributes = getProfileAttributes(mProvider);
                    for (TreeMap.Entry<String, ProfileUploadDataPair> attribute : profileAttributes.entrySet())
                    {
                        final String customerID = attribute.getKey();
                        final JSONObject consolidatedJSON = attribute.getValue().consolidatedJSON;
                        JSONObject uploadJSON = new JSONObject();
                        uploadJSON.put(JsonObjects.ProfileUpload.KEY_JSON_ATTRIBUTE, consolidatedJSON);
                        final StringBuilder rows = attribute.getValue().rowIDs;
                        final String uploadString = uploadJSON.toString();

                        if (upload(UploadType.PROFILES, String.format(PROFILE_URL, Constants.PROFILES_URL, apiKey, URLEncoder.encode(customerID, "UTF-8")), uploadString, mInstallId, apiKey, customerID))
                        {
                            mProvider.runBatchTransaction(new Runnable()
                            {
                                public void run()
                                {
                                    deleteProfileAttribute(mProvider, rows);
                                }
                            });
                        }
                    }
                    if (null != callback)
                    {
                        /*
                        * Execute the callback on a separate thread, to avoid exposing this thread to the client of the
                        * library
                        */
                        new Thread(callback, UPLOAD_PROFILE_CALLBACK_THREAD_NAME).start();
                    }

                    mSessionHandler.sendEmptyMessage(SessionHandler.MESSAGE_UPLOAD_PROFILE_CALLBACK);
                    break;
                }
                case MESSAGE_RETRY_UPLOAD_PROFILE_REQUEST:
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.d(Constants.LOG_TAG, "Received MESSAGE_RETRY_UPLOAD_PROFILE_REQUEST"); //$NON-NLS-1$
                    }

                    mSessionHandler.sendMessage(mSessionHandler.obtainMessage(SessionHandler.MESSAGE_UPLOAD_PROFILE, msg.obj));
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

    private boolean isUploadTooBig(final String uploadString)
    {
        byte[] uploadBytes = uploadString.getBytes();
        boolean tooBig = uploadBytes.length >= 50000;
        if (tooBig && Constants.IS_LOGGABLE)
        {
            Log.w(Constants.LOG_TAG, "Attempting to upload too much data. Abandoning upload and deleting data."); //$NON-NLS-1$
        }
        return tooBig;
    }
    
    /**
     * This method is called when the session uploading is successful and the AMP response is received.
     * 
     * @param response AMP response received from the HTTP .
     */
    protected void onUploadResponded(final String response)
    {
    }

    /**
     * This method is called when the profile uploading is successful.
     *
     * @param response Response received from the HTTP .
     */
    protected void onProfileUploadResponded(final String response)
    {
        if (Constants.IS_LOGGABLE)
        {
            Log.v(Constants.LOG_TAG, String.format("Profile response is: %s", response)); //$NON-NLS-1$
        }
    }

    private enum UploadType { SESSIONS, PROFILES }

    /**
     * Uploads the post Body to the webservice
     *
     * @param url where {@code body} will be posted to. Cannot be null.
     * @param body sessions/profile upload body as a string. This should be a plain old string. Cannot be null.
     * @return True on success, false on failure.
     */
    private boolean upload(UploadType uploadType, final String url, final String body, final String installId, final String apiKey, final String customerID)
    {
        if (Constants.IS_PARAMETER_CHECKING_ENABLED)
        {
            if (null == url)
            {
                throw new IllegalArgumentException("url cannot be null"); //$NON-NLS-1$
            }

            if (null == body)
            {
                throw new IllegalArgumentException("body cannot be null"); //$NON-NLS-1$
            }
        }

        if (Constants.IS_LOGGABLE)
        {
            if (uploadType == UploadType.SESSIONS)
            {
                Log.v(Constants.LOG_TAG, String.format("Upload body before compression is: %s", body)); //$NON-NLS-1$
            }
            else
            {
                Log.v(Constants.LOG_TAG, String.format("Profile upload body is: %s", body)); //$NON-NLS-1$
            }
        }

        /*
         * As per Google's documentation, use HttpURLConnection for API 9 and greater and DefaultHttpClient for API 8 and
         * lower. <http://android-developers.blogspot.com/2011/09/androids-http-clients.html>. HTTP library.
         *
         * Note: HTTP GZIP compression is explicitly disabled. Instead, the uploaded data (for sessions) is already GZIPPED before it is put
         * into the HTTP post.
         */
        byte[] originalBytes;
        final byte[] uploadData;
        {
            GZIPOutputStream gos = null;
            try
            {
                originalBytes = body.getBytes("UTF-8"); //$NON-NLS-1$
                if (uploadType == UploadType.SESSIONS)
                {
                    /*
                    * GZIP the data to upload
                    */
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream(originalBytes.length);
                    gos = new GZIPOutputStream(baos);
                    gos.write(originalBytes);
                    gos.finish();

                    /*
                     * KitKat throws an exception when you call flush
                     * https://code.google.com/p/android/issues/detail?id=62589
                     */
                    if (DatapointHelper.getApiLevel() < 19)
                    {
                        gos.flush();
                    }

                    uploadData = baos.toByteArray();
                }
                else
                {
                    uploadData = originalBytes;
                }
            }
            catch (final UnsupportedEncodingException e)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.w(Constants.LOG_TAG, "UnsupportedEncodingException", e); //$NON-NLS-1$
                }
                return false;
            }
            catch (final IOException e)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.w(Constants.LOG_TAG, "IOException", e); //$NON-NLS-1$
                }
                return false;
            }
            finally
            {
                if (null != gos)
                {
                    try
                    {
                        gos.close();
                        gos = null;
                    }
                    catch (final IOException e)
                    {
                        if (Constants.IS_LOGGABLE)
                        {
                            Log.w(Constants.LOG_TAG, "Caught exception", e); //$NON-NLS-1$
                        }

                        return false;
                    }
                }
            }
        }

        if (DatapointHelper.getApiLevel() >= 9)
        {
            HttpURLConnection connection = null;
            try
            {
                connection = (HttpURLConnection) new URL(url).openConnection();

                connection.setConnectTimeout((int) DateUtils.MINUTE_IN_MILLIS);
                connection.setReadTimeout((int) DateUtils.MINUTE_IN_MILLIS);
                connection.setDoOutput(true); // sets POST method implicitly
                if (uploadType == UploadType.SESSIONS)
                {
                    connection.setRequestProperty("Content-Type", "application/x-gzip"); //$NON-NLS-1$//$NON-NLS-2$
                    connection.setRequestProperty("Content-Encoding", "gzip"); //$NON-NLS-1$//$NON-NLS-2$
                }
                else
                {
                    connection.setRequestProperty("Content-Type", "application/json; charset=utf-8"); //$NON-NLS-1$//$NON-NLS-2$
                }
                /*
                 * Workaround for EOFExceptions
                 * http://stackoverflow.com/questions/17638398/androids-httpurlconnection-throws-eofexception-on-head-requests
                 */
                connection.setRequestProperty("Accept-Encoding", ""); //
                connection.setRequestProperty("x-upload-time", Long.toString(Math.round((double) System.currentTimeMillis() / DateUtils.SECOND_IN_MILLIS))); //$NON-NLS-1$//$NON-NLS-2$
                connection.setRequestProperty("x-install-id", installId); //$NON-NLS-1$
                connection.setRequestProperty("x-app-id", apiKey); //$NON-NLS-1$
                connection.setRequestProperty("x-client-version", Constants.LOCALYTICS_CLIENT_LIBRARY_VERSION); //$NON-NLS-1$
                connection.setRequestProperty("x-app-version", DatapointHelper.getAppVersion(mContext)); //$NON-NLS-1$
                connection.setRequestProperty("x-customer-id", customerID); //$NON-NLS-1$
                connection.setFixedLengthStreamingMode(uploadData.length);

                OutputStream stream = null;
                try
                {
                    stream = connection.getOutputStream();

                    stream.write(uploadData);
                }
                finally
                {
                    if (null != stream)
                    {
                        stream.flush();
                        stream.close();
                        stream = null;
                    }
                }

                final int statusCode = connection.getResponseCode();

                if (Constants.IS_LOGGABLE)
                {
                    if (uploadType == UploadType.SESSIONS)
                    {
                        Log.v(Constants.LOG_TAG, String.format("Upload complete with status %d", Integer.valueOf(statusCode))); //$NON-NLS-1$
                    }
                    else
                    {
                        Log.v(Constants.LOG_TAG, String.format("Profile upload complete with status %d", Integer.valueOf(statusCode))); //$NON-NLS-1$
                    }
                }

                /*
                 * 4xx status codes indicate a user error, so upload should NOT be reattempted
                 */
                if (statusCode >= 400 && statusCode <= 499)
                {
                    return true;
                }

                /*
                 * 5xx status codes indicate a server error, so upload should be reattempted
                 */
                if (statusCode >= 500 && statusCode <= 599)
                {
                    return false;
                }

                /*
                 * Retrieves the HTTP response as a string if available
                 */
                retrieveHttpResponse(uploadType, connection.getInputStream());
            }
            catch (final MalformedURLException e)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.w(Constants.LOG_TAG, "ClientProtocolException", e); //$NON-NLS-1$
                }

                return false;
            }
            catch (final IOException e)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.w(Constants.LOG_TAG, "ClientProtocolException", e); //$NON-NLS-1$
                }

                return false;
            }
            finally
            {
                if (null != connection)
                {
                    connection.disconnect();
                    connection = null;
                }
            }
        }
        else
        {
            /*
             * Note: DefaultHttpClient appears to sometimes cause an OutOfMemory error. Although we've seen exceptions from
             * the wild, it isn't clear whether this is due to a bug in DefaultHttpClient or just a random error that has
             * occurred once or twice due to buggy devices.
             */
            HttpParams httpParameters = new BasicHttpParams();
            // Set the timeout in milliseconds until a connection is established.
            HttpConnectionParams.setConnectionTimeout(httpParameters, (int) DateUtils.MINUTE_IN_MILLIS);
            // Set the default socket timeout (SO_TIMEOUT)
            // in milliseconds which is the timeout for waiting for data.
            HttpConnectionParams.setSoTimeout(httpParameters, (int) DateUtils.MINUTE_IN_MILLIS);

            final DefaultHttpClient client = new DefaultHttpClient(httpParameters);
            final HttpPost method = new HttpPost(url);
            if (uploadType == UploadType.SESSIONS)
            {
                method.addHeader("Content-Type", "application/x-gzip"); //$NON-NLS-1$ //$NON-NLS-2$
                method.addHeader("Content-Encoding", "gzip"); //$NON-NLS-1$//$NON-NLS-2$
            }
            else
            {
                method.addHeader("Content-Type", "application/json; charset=utf-8"); //$NON-NLS-1$ //$NON-NLS-2$
            }
            method.addHeader("x-upload-time", Long.toString(Math.round((double) System.currentTimeMillis() / DateUtils.SECOND_IN_MILLIS))); //$NON-NLS-1$//$NON-NLS-2$
            method.addHeader("x-install-id", installId); //$NON-NLS-1$
            method.addHeader("x-app-id", apiKey); //$NON-NLS-1$
            method.addHeader("x-client-version", Constants.LOCALYTICS_CLIENT_LIBRARY_VERSION); //$NON-NLS-1$
            method.addHeader("x-app-version", DatapointHelper.getAppVersion(mContext)); //$NON-NLS-1$
            method.addHeader("x-customer-id", customerID); //$NON-NLS-1$

            try
            {
                final ByteArrayEntity patchBody = new ByteArrayEntity(uploadData);
                method.setEntity(patchBody);

                final HttpResponse response = client.execute(method);

                final StatusLine status = response.getStatusLine();
                final int statusCode = status.getStatusCode();

                if (Constants.IS_LOGGABLE)
                {
                    if (uploadType == UploadType.SESSIONS)
                    {
                        Log.v(Constants.LOG_TAG, String.format("Upload complete with status %d", Integer.valueOf(statusCode))); //$NON-NLS-1$
                    }
                    else
                    {
                        Log.v(Constants.LOG_TAG, String.format("Profile upload complete with status %d", Integer.valueOf(statusCode))); //$NON-NLS-1$
                    }
                }

                /*
                 * 4xx status codes indicate a user error, so upload should NOT be reattempted
                 */
                if (statusCode >= 400 && statusCode <= 499)
                {
                    return true;
                }

                /*
                 * 5xx status codes indicate a server error, so upload should be reattempted
                 */
                if (statusCode >= 500 && statusCode <= 599)
                {
                    return false;
                }

                /*
                * Retrieves the HTTP response as a string if available
                */
                HttpEntity entry = response.getEntity();
                if (null != entry)
                {
                    retrieveHttpResponse(uploadType, entry.getContent());
                }
            }
            catch (final ClientProtocolException e)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.w(Constants.LOG_TAG, "ClientProtocolException", e); //$NON-NLS-1$
                }
                return false;
            }
            catch (final IOException e)
            {
                if (Constants.IS_LOGGABLE)
                {
                    Log.w(Constants.LOG_TAG, "IOException", e); //$NON-NLS-1$
                }
                return false;
            }
        }

        return true;
    }

    /**
     * Retrieves the HTTP response body as a string and trigger onUploadResponded
     *
     * @param input InputStream from which the HTTP response body can be fetched. Cannot be null.
     */
    /* package */private void retrieveHttpResponse(UploadType uploadType, final InputStream input) throws IOException
    {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(input, "UTF-8"));
        final StringBuilder builder = new StringBuilder();

        String line = null;
        while ((line = reader.readLine()) != null)
        {
            builder.append(line);
        }

        final String response = builder.toString();
        if (!TextUtils.isEmpty(response))
        {
            if (uploadType == UploadType.SESSIONS)
            {
                onUploadResponded(response);
            }
            else
            {
                onProfileUploadResponded(response);
            }
        }

        reader.close();
    }

    /**
     * Helper that converts blobs in the database into a JSON representation for upload.
     *
     * @return A list of JSON objects to upload to the server
     */
    /* package */static List<JSONObject> convertDatabaseToJson(final Context context, final LocalyticsProvider provider, final String apiKey)
    {
        final List<JSONObject> result = new LinkedList<JSONObject>();
        Cursor cursor = null;
        try
        {
            cursor = provider.query(UploadBlobsDbColumns.TABLE_NAME, null, null, null, null);

            final long creationTime = getApiKeyCreationTime(provider, apiKey);

            final int idColumn = cursor.getColumnIndexOrThrow(UploadBlobsDbColumns._ID);
            final int uuidColumn = cursor.getColumnIndexOrThrow(UploadBlobsDbColumns.UUID);
            while (cursor.moveToNext())
            {
                try
                {
                    final JSONObject blobHeader = new JSONObject();

                    blobHeader.put(JsonObjects.BlobHeader.KEY_DATA_TYPE, BlobHeader.VALUE_DATA_TYPE);
                    blobHeader.put(JsonObjects.BlobHeader.KEY_PERSISTENT_STORAGE_CREATION_TIME_SECONDS, creationTime);
                    blobHeader.put(JsonObjects.BlobHeader.KEY_SEQUENCE_NUMBER, cursor.getLong(idColumn));
                    blobHeader.put(JsonObjects.BlobHeader.KEY_UNIQUE_ID, cursor.getString(uuidColumn));
                    blobHeader.put(JsonObjects.BlobHeader.KEY_ATTRIBUTES, getAttributesFromSession(provider, apiKey, getSessionIdForBlobId(provider, cursor.getLong(idColumn))));
                    
                    final JSONObject identifiers = getIdentifiers(provider);
                    if (null != identifiers)
                    {
                    	blobHeader.put(JsonObjects.BlobHeader.KEY_IDENTIFIERS, identifiers);
                    }
                    
                    result.add(blobHeader);
                    
                    if (Constants.IS_LOGGABLE)
                    {
                    	Log.w(Constants.LOG_TAG, result.toString());
                    }

                    Cursor blobEvents = null;
                    try
                    {
                        blobEvents = provider.query(UploadBlobEventsDbColumns.TABLE_NAME, new String[]
                            { UploadBlobEventsDbColumns.EVENTS_KEY_REF }, String.format("%s = ?", UploadBlobEventsDbColumns.UPLOAD_BLOBS_KEY_REF), new String[] //$NON-NLS-1$
                            { Long.toString(cursor.getLong(idColumn)) }, UploadBlobEventsDbColumns.EVENTS_KEY_REF);

                        final int eventIdColumn = blobEvents.getColumnIndexOrThrow(UploadBlobEventsDbColumns.EVENTS_KEY_REF);
                        while (blobEvents.moveToNext())
                        {
                            result.add(convertEventToJson(provider, context, blobEvents.getLong(eventIdColumn), cursor.getLong(idColumn), apiKey));
                        }
                    }
                    finally
                    {
                        if (null != blobEvents)
                        {
                            blobEvents.close();
                        }
                    }
                }
                catch (final JSONException e)
                {
                    if (Constants.IS_LOGGABLE)
                    {
                        Log.w(Constants.LOG_TAG, "Caught exception", e); //$NON-NLS-1$
                    }
                }
            }
        }
        finally
        {
            if (cursor != null)
            {
                cursor.close();
                cursor = null;
            }
        }

        if (Constants.IS_LOGGABLE)
        {
            Log.v(Constants.LOG_TAG, String.format("JSON result is %s", result.toString())); //$NON-NLS-1$
        }

        return result;
    }

    /* package */static TreeMap<String, ProfileUploadDataPair> getProfileAttributes(final LocalyticsProvider provider)
    {
        TreeMap<String, ProfileUploadDataPair> profileAttributes = new TreeMap<String, ProfileUploadDataPair>();
        Cursor cursor = null;
        try
        {
            cursor = provider.query(ProfileDbColumns.TABLE_NAME, null, null, null, ProfileDbColumns._ID + " ASC");

            while (cursor.moveToNext())
            {
                final int operation = cursor.getInt(cursor.getColumnIndexOrThrow(ProfileDbColumns.ACTION));
                if (operation == LocalyticsSession.ProfileDbAction.SET_ATTRIBUTE.ordinal())
                {
                    final String customerID = cursor.getString(cursor.getColumnIndexOrThrow(ProfileDbColumns.CUSTOMER_ID));
                    final String attributeString = cursor.getString(cursor.getColumnIndexOrThrow(ProfileDbColumns.ATTRIBUTE));
                    final String rowID = String.valueOf(cursor.getInt(cursor.getColumnIndexOrThrow(ProfileDbColumns._ID)));

                    ProfileUploadDataPair tupleForCustomer = profileAttributes.get(customerID);
                    JSONObject attributeJSON;
                    try
                    {
                        attributeJSON = new JSONObject(attributeString);
                    }
                    catch (JSONException e)
                    {
                        attributeJSON = new JSONObject();
                        if (Constants.IS_LOGGABLE)
                        {
                            Log.w(Constants.LOG_TAG, "Profile attribute is unreadable; data will not be uploaded but will be deleted."); //$NON-NLS-1$
                        }
                    }

                    if (null == tupleForCustomer)
                    {
                        tupleForCustomer = new ProfileUploadDataPair(attributeJSON, new StringBuilder(rowID));
                        profileAttributes.put(customerID, tupleForCustomer);
                    }
                    else
                    {
                        tupleForCustomer.rowIDs.append(", ").append(rowID);
                        try
                        {
                            String key = (String) attributeJSON.keys().next();
                            tupleForCustomer.consolidatedJSON.put(key, attributeJSON.get(key));
                        }
                        catch (Exception e)
                        {}
                    }
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

        return profileAttributes;
    }

    private static class ProfileUploadDataPair
    {
        public JSONObject consolidatedJSON;
        public StringBuilder rowIDs;

        public ProfileUploadDataPair(final JSONObject json, final StringBuilder rows)
        {
            consolidatedJSON = json;
            rowIDs = rows;
        }
    }

    /* package */static String getCustomerID(final LocalyticsProvider provider)
    {
        return provider.getUserIdAndType().get(LocalyticsProvider.USER_ID);
    }

    /* package */static void deleteProfileAttribute(final LocalyticsProvider provider, final StringBuilder rowsToDelete)
    {
        provider.remove(ProfileDbColumns.TABLE_NAME, String.format("%s IN (%s)", ProfileDbColumns._ID, rowsToDelete.toString()), null);
    }

    /**
     * Deletes all blobs and sessions/events/attributes associated with those blobs.
     * <p>
     * This should be called after a successful upload completes.
     *
     * @param provider Localytics database provider. Cannot be null.
     */
    /* package */static void deleteBlobsAndSessions(final LocalyticsProvider provider)
    {
        /*
         * Deletion needs to occur in a specific order due to database constraints. Specifically, blobevents need to be
         * deleted first. Then blobs themselves can be deleted. Then attributes need to be deleted first. Then events. Then
         * sessions.
         */

        final LinkedList<Long> sessionsToDelete = new LinkedList<Long>();
        final HashSet<Long> blobsToDelete = new HashSet<Long>();

        Cursor blobEvents = null;
        try
        {
            blobEvents = provider.query(UploadBlobEventsDbColumns.TABLE_NAME, new String[]
                {
                    UploadBlobEventsDbColumns._ID,
                    UploadBlobEventsDbColumns.EVENTS_KEY_REF,
                    UploadBlobEventsDbColumns.UPLOAD_BLOBS_KEY_REF }, null, null, null);

            final int uploadBlobIdColumn = blobEvents.getColumnIndexOrThrow(UploadBlobEventsDbColumns.UPLOAD_BLOBS_KEY_REF);
            final int blobEventIdColumn = blobEvents.getColumnIndexOrThrow(UploadBlobEventsDbColumns._ID);
            final int eventIdColumn = blobEvents.getColumnIndexOrThrow(UploadBlobEventsDbColumns.EVENTS_KEY_REF);
            while (blobEvents.moveToNext())
            {
                final long blobId = blobEvents.getLong(uploadBlobIdColumn);
                final long blobEventId = blobEvents.getLong(blobEventIdColumn);
                final long eventId = blobEvents.getLong(eventIdColumn);

                // delete the blobevent
                provider.remove(UploadBlobEventsDbColumns.TABLE_NAME, String.format("%s = ?", UploadBlobEventsDbColumns._ID), new String[] { Long.toString(blobEventId) }); //$NON-NLS-1$

                /*
                 * Add the blob to the list of blobs to be deleted
                 */
                blobsToDelete.add(Long.valueOf(blobId));

                // delete all attributes for the event
                provider.remove(AttributesDbColumns.TABLE_NAME, String.format("%s = ?", AttributesDbColumns.EVENTS_KEY_REF), new String[] { Long.toString(eventId) }); //$NON-NLS-1$

                /*
                 * Check to see if the event is a close event, indicating that the session is complete and can also be deleted
                 */
                Cursor eventCursor = null;
                try
                {
                    eventCursor = provider.query(EventsDbColumns.TABLE_NAME, new String[]
                        { EventsDbColumns.SESSION_KEY_REF }, String.format("%s = ? AND %s = ?", EventsDbColumns._ID, EventsDbColumns.EVENT_NAME), new String[] //$NON-NLS-1$
                        {
                            Long.toString(eventId),
                            Constants.CLOSE_EVENT }, null);

                    if (eventCursor.moveToFirst())
                    {
                        final long sessionId = eventCursor.getLong(eventCursor.getColumnIndexOrThrow(EventsDbColumns.SESSION_KEY_REF));

                        provider.remove(EventHistoryDbColumns.TABLE_NAME, String.format("%s = ?", EventHistoryDbColumns.SESSION_KEY_REF), new String[] //$NON-NLS-1$
                            { Long.toString(sessionId) });

                        sessionsToDelete.add(Long.valueOf(eventCursor.getLong(eventCursor.getColumnIndexOrThrow(EventsDbColumns.SESSION_KEY_REF))));
                    }
                }
                finally
                {
                    if (null != eventCursor)
                    {
                        eventCursor.close();
                        eventCursor = null;
                    }
                }

                // delete the event
                provider.remove(EventsDbColumns.TABLE_NAME, String.format("%s = ?", EventsDbColumns._ID), new String[] { Long.toString(eventId) }); //$NON-NLS-1$
            }
        }
        finally
        {
            if (null != blobEvents)
            {
                blobEvents.close();
                blobEvents = null;
            }
        }

        // delete blobs
        for (final long x : blobsToDelete)
        {
            provider.remove(UploadBlobsDbColumns.TABLE_NAME, String.format("%s = ?", UploadBlobsDbColumns._ID), new String[] { Long.toString(x) }); //$NON-NLS-1$
        }

        // delete sessions
        for (final long x : sessionsToDelete)
        {
            provider.remove(SessionsDbColumns.TABLE_NAME, String.format("%s = ?", SessionsDbColumns._ID), new String[] { Long.toString(x) }); //$NON-NLS-1$
        }
    }
    
    /**
     * Gets the creation time for an API key.
     *
     * @param provider Localytics database provider. Cannot be null.
     * @param key Localytics API key. Cannot be null.
     * @return The time in seconds since the Unix Epoch when the API key entry was created in the database.
     * @throws RuntimeException if the API key entry doesn't exist in the database.
     */
    /* package */static long getApiKeyCreationTime(final LocalyticsProvider provider, final String key)
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(ApiKeysDbColumns.TABLE_NAME, null, String.format("%s = ?", ApiKeysDbColumns.API_KEY), new String[] { key }, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
                return Math.round((float) cursor.getLong(cursor.getColumnIndexOrThrow(ApiKeysDbColumns.CREATED_TIME)) / DateUtils.SECOND_IN_MILLIS);
            }

            /*
             * This should never happen
             */
            throw new RuntimeException("API key entry couldn't be found"); //$NON-NLS-1$
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
     * Helper method to generate the attributes object for a session
     *
     * @param provider Instance of the Localytics database provider. Cannot be null.
     * @param apiKey Localytics API key. Cannot be null.
     * @param sessionId The {@link SessionsDbColumns#_ID} of the session.
     * @return a JSONObject representation of the session attributes
     * @throws JSONException if a problem occurred converting the element to JSON.
     */
    /* package */static JSONObject getAttributesFromSession(final LocalyticsProvider provider, final String apiKey, final long sessionId) throws JSONException
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(SessionsDbColumns.TABLE_NAME, null, String.format("%s = ?", SessionsDbColumns._ID), new String[] { Long.toString(sessionId) }, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
                final JSONObject result = new JSONObject();
                
                // Sessions table
                result.put(JsonObjects.BlobHeader.Attributes.KEY_CLIENT_APP_VERSION, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.APP_VERSION)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_DATA_CONNECTION, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.NETWORK_TYPE)));
                String deviceID = cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_ANDROID_ID_HASH));
                if (!"".equals(deviceID)) {
                    result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_ANDROID_ID_HASH, deviceID);
                }
                result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_COUNTRY, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_COUNTRY)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_MANUFACTURER, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_MANUFACTURER)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_MODEL, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_MODEL)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_OS_VERSION, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.ANDROID_VERSION)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_PLATFORM, JsonObjects.BlobHeader.Attributes.VALUE_PLATFORM);
                result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_SERIAL_HASH, cursor.isNull(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_SERIAL_NUMBER_HASH)) ? JSONObject.NULL
                        : cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_SERIAL_NUMBER_HASH)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_SDK_LEVEL, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.ANDROID_SDK)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_LOCALYTICS_API_KEY, apiKey);
                result.put(JsonObjects.BlobHeader.Attributes.KEY_LOCALYTICS_CLIENT_LIBRARY_VERSION, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.LOCALYTICS_LIBRARY_VERSION)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_LOCALYTICS_DATA_TYPE, JsonObjects.BlobHeader.Attributes.VALUE_DATA_TYPE);
                result.put(JsonObjects.BlobHeader.Attributes.KEY_CURRENT_ANDROID_ID, cursor.isNull(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_ANDROID_ID)) ? JSONObject.NULL
                        : cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_ANDROID_ID)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_CURRENT_ADVERTISING_ID, cursor.isNull(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_ADVERTISING_ID)) ? JSONObject.NULL
                        : cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.DEVICE_ADVERTISING_ID)));

                // This would only be null after an upgrade from an earlier version of the Localytics library
                final String installationID = cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.LOCALYTICS_INSTALLATION_ID));
                if (null != installationID)
                {
                    result.put(JsonObjects.BlobHeader.Attributes.KEY_LOCALYTICS_INSTALLATION_ID, installationID);
                }
                result.put(JsonObjects.BlobHeader.Attributes.KEY_LOCALE_COUNTRY, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.LOCALE_COUNTRY)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_LOCALE_LANGUAGE, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.LOCALE_LANGUAGE)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_NETWORK_CARRIER, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.NETWORK_CARRIER)));
                result.put(JsonObjects.BlobHeader.Attributes.KEY_NETWORK_COUNTRY, cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.NETWORK_COUNTRY)));
                                                
                // Info table
                String fbAttribution = getStringFromAppInfo(provider, InfoDbColumns.FB_ATTRIBUTION);
                if (null != fbAttribution)
                {
                	result.put(JsonObjects.BlobHeader.Attributes.KEY_FB_COOKIE, fbAttribution);
                }
                
                String playAttribution = getStringFromAppInfo(provider, InfoDbColumns.PLAY_ATTRIBUTION);
                if (null != playAttribution)
                {
                	result.put(JsonObjects.BlobHeader.Attributes.KEY_GOOGLE_PLAY_ATTRIBUTION, playAttribution);
                }
                
                String registrationId = getStringFromAppInfo(provider, InfoDbColumns.REGISTRATION_ID);
                if (null != registrationId)
                {
                	result.put(JsonObjects.BlobHeader.Attributes.KEY_PUSH_ID, registrationId);
                }

                String firstAndroidId = getStringFromAppInfo(provider, InfoDbColumns.FIRST_ANDROID_ID);
                if (null != firstAndroidId)
                {
                	result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_ANDROID_ID, firstAndroidId);
                }

                String firstAdvertisingId = getStringFromAppInfo(provider, InfoDbColumns.FIRST_ADVERTISING_ID);
                if (null != firstAdvertisingId)
                {
                    result.put(JsonObjects.BlobHeader.Attributes.KEY_DEVICE_ADVERTISING_ID, firstAdvertisingId);
                }
                
                String packageName = getStringFromAppInfo(provider, InfoDbColumns.PACKAGE_NAME);
                if (null != packageName)
                {
                	result.put(JsonObjects.BlobHeader.Attributes.KEY_PACKAGE_NAME, packageName);
                }
                
                return result;
            }

            throw new RuntimeException("No session exists"); //$NON-NLS-1$
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
     * Helper method to generate the attributes object for a session
     *
     * @param provider Instance of the Localytics database provider. Cannot be null.
     * @return a JSONObject representation of the session attributes
     * @throws JSONException if a problem occurred converting the element to JSON.
     */
    /* package */static JSONObject getIdentifiers(final LocalyticsProvider provider) throws JSONException
    {
        Cursor cursor = null;
        try
        {            	
            cursor = provider.query(IdentifiersDbColumns.TABLE_NAME, null, null, null, null); //$NON-NLS-1$

            JSONObject result = null;
            
            while (cursor.moveToNext())
            {
            	if (null == result)
            	{
            		result = new JSONObject();
            	}
            	
            	result.put(cursor.getString(cursor.getColumnIndexOrThrow(IdentifiersDbColumns.KEY)), cursor.getString(cursor.getColumnIndexOrThrow(IdentifiersDbColumns.VALUE)));
            }
            
            return result;
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
     * Converts an event into a JSON object.
     * <p>
     * There are three types of events: open, close, and application. Open and close events are Localytics events, while
     * application events are generated by the app. The return value of this method will vary based on the type of event that
     * is being converted.
     *
     * @param provider Localytics database instance. Cannot be null.
     * @param context Application context. Cannot be null.
     * @param eventId {@link EventsDbColumns#_ID} of the event to convert.
     * @param blobId {@link UploadBlobEventsDbColumns#_ID} of the upload blob that contains this event.
     * @param apiKey the Localytics API key. Cannot be null.
     * @return JSON representation of the event.
     * @throws JSONException if a problem occurred converting the element to JSON.
     */
    /* package */static JSONObject convertEventToJson(final LocalyticsProvider provider, final Context context, final long eventId, final long blobId, final String apiKey)
                                                                                                                                                                           throws JSONException
    {
        final JSONObject result = new JSONObject();

        Cursor cursor = null;

        try
        {
            cursor = provider.query(EventsDbColumns.TABLE_NAME, null, String.format("%s = ?", EventsDbColumns._ID), new String[] //$NON-NLS-1$
                { Long.toString(eventId) }, EventsDbColumns._ID);

            if (cursor.moveToFirst())
            {
                final String eventName = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.EVENT_NAME));
                final long sessionId = getSessionIdForEventId(provider, eventId);
                final String sessionUuid = getSessionUuid(provider, sessionId);
                final long sessionStartTime = getSessionStartTime(provider, sessionId);

                if (Constants.OPEN_EVENT.equals(eventName))
                {
                    result.put(JsonObjects.SessionOpen.KEY_DATA_TYPE, JsonObjects.SessionOpen.VALUE_DATA_TYPE);
                    result.put(JsonObjects.SessionOpen.KEY_WALL_TIME_SECONDS, Math.round((double) cursor.getLong(cursor.getColumnIndex(EventsDbColumns.WALL_TIME))
                            / DateUtils.SECOND_IN_MILLIS));
                    result.put(JsonObjects.SessionOpen.KEY_EVENT_UUID, sessionUuid);

                    final long elapsedTime = getElapsedTimeSinceLastSession(provider, sessionId);
                    if (elapsedTime > 0L) {
                        result.put(JsonObjects.SessionOpen.KEY_TIME_SINCE_LAST, Math.round(elapsedTime / DateUtils.SECOND_IN_MILLIS));
                    }

                    /*
                     * Both the database and the web service use 1-based indexing.
                     */
                    result.put(JsonObjects.SessionOpen.KEY_COUNT, sessionId);

                    /*
                     * Append lat/lng if it is available
                     */
                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.LAT_NAME)) &&
                        false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.LNG_NAME)))
                    {
                    	double lat = cursor.getDouble(cursor.getColumnIndexOrThrow(EventsDbColumns.LAT_NAME));
                    	double lng = cursor.getDouble(cursor.getColumnIndexOrThrow(EventsDbColumns.LNG_NAME));
                    	
                    	if (lat != 0 && lng != 0)
                    	{
                    		result.put(JsonObjects.SessionEvent.KEY_LATITUDE, lat);
                    		result.put(JsonObjects.SessionEvent.KEY_LONGITUDE, lng);
                    	}
                    }
                    
                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.CUST_ID)))
                    {
                        String customerID = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.CUST_ID));
                        String userType = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.USER_TYPE));

                        result.put(JsonObjects.SessionEvent.CUST_ID, customerID);
                        result.put(JsonObjects.SessionEvent.USER_TYPE, userType);
                    }

                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.IDENTIFIERS)))
                    {
                        String ids = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.IDENTIFIERS));
                        result.put(JsonObjects.SessionEvent.IDENTIFIERS, new JSONObject(ids));
                    }

                    /*
                     * Get the custom dimensions from the attributes table
                     */
                    Cursor attributesCursor = null;
                    try
                    {
                        attributesCursor = provider.query(AttributesDbColumns.TABLE_NAME, null, String.format("%s = ?", AttributesDbColumns.EVENTS_KEY_REF), new String[] { Long.toString(eventId) }, null); //$NON-NLS-1$

                        final int keyColumn = attributesCursor.getColumnIndexOrThrow(AttributesDbColumns.ATTRIBUTE_KEY);
                        final int valueColumn = attributesCursor.getColumnIndexOrThrow(AttributesDbColumns.ATTRIBUTE_VALUE);
                        while (attributesCursor.moveToNext())
                        {
                            final String key = attributesCursor.getString(keyColumn);
                            final String value = attributesCursor.getString(valueColumn);

                            if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_1, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_2, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_3, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_4, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_5, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_6, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_7, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_8, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_9, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_10, value);
                            }
                        }
                    }
                    finally
                    {
                        if (null != attributesCursor)
                        {
                            attributesCursor.close();
                            attributesCursor = null;
                        }
                    }
                }
                else if (Constants.CLOSE_EVENT.equals(eventName))
                {
                    result.put(JsonObjects.SessionClose.KEY_DATA_TYPE, JsonObjects.SessionClose.VALUE_DATA_TYPE);
                    result.put(JsonObjects.SessionClose.KEY_EVENT_UUID, cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.UUID)));
                    result.put(JsonObjects.SessionClose.KEY_SESSION_UUID, sessionUuid);
                    result.put(JsonObjects.SessionClose.KEY_SESSION_START_TIME, Math.round((double) sessionStartTime / DateUtils.SECOND_IN_MILLIS));
                    result.put(JsonObjects.SessionClose.KEY_WALL_TIME_SECONDS, Math.round((double) cursor.getLong(cursor.getColumnIndex(EventsDbColumns.WALL_TIME))
                            / DateUtils.SECOND_IN_MILLIS));

                    /*
                     * length is a special case, as it depends on the start time embedded in the session table
                     */
                    Cursor sessionCursor = null;
                    try
                    {
                        sessionCursor = provider.query(SessionsDbColumns.TABLE_NAME, new String[]
                            { SessionsDbColumns.SESSION_START_WALL_TIME }, String.format("%s = ?", SessionsDbColumns._ID), new String[] { Long.toString(cursor.getLong(cursor.getColumnIndexOrThrow(EventsDbColumns.SESSION_KEY_REF))) }, null); //$NON-NLS-1$

                        if (sessionCursor.moveToFirst())
                        {
                            result.put(JsonObjects.SessionClose.KEY_SESSION_LENGTH_SECONDS, Math.round((double) cursor.getLong(cursor.getColumnIndex(EventsDbColumns.WALL_TIME))
                                    / DateUtils.SECOND_IN_MILLIS)
                                    - Math.round((double) sessionCursor.getLong(sessionCursor.getColumnIndexOrThrow(SessionsDbColumns.SESSION_START_WALL_TIME))
                                            / DateUtils.SECOND_IN_MILLIS));
                        }
                        else
                        {
                            // this should never happen
                            throw new RuntimeException("Session didn't exist"); //$NON-NLS-1$
                        }
                    }
                    finally
                    {
                        if (null != sessionCursor)
                        {
                            sessionCursor.close();
                            sessionCursor = null;
                        }
                    }

                    /*
                     * The close also contains a special case element for the screens history
                     */
                    Cursor eventHistoryCursor = null;
                    try
                    {
                        eventHistoryCursor = provider.query(EventHistoryDbColumns.TABLE_NAME, new String[]
                            { EventHistoryDbColumns.NAME }, String.format("%s = ? AND %s = ?", EventHistoryDbColumns.SESSION_KEY_REF, EventHistoryDbColumns.TYPE), new String[] { Long.toString(sessionId), Integer.toString(EventHistoryDbColumns.TYPE_SCREEN) }, EventHistoryDbColumns._ID); //$NON-NLS-1$

                        final JSONArray screens = new JSONArray();
                        while (eventHistoryCursor.moveToNext())
                        {
                            screens.put(eventHistoryCursor.getString(eventHistoryCursor.getColumnIndexOrThrow(EventHistoryDbColumns.NAME)));
                        }

                        if (screens.length() > 0)
                        {
                            result.put(JsonObjects.SessionClose.KEY_FLOW_ARRAY, screens);
                        }
                    }
                    finally
                    {
                        if (null != eventHistoryCursor)
                        {
                            eventHistoryCursor.close();
                            eventHistoryCursor = null;
                        }
                    }

                    /*
                     * Append lat/lng if it is available
                     */
                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.LAT_NAME)) &&
                        false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.LNG_NAME)))
                    {
                    	double lat = cursor.getDouble(cursor.getColumnIndexOrThrow(EventsDbColumns.LAT_NAME));
                    	double lng = cursor.getDouble(cursor.getColumnIndexOrThrow(EventsDbColumns.LNG_NAME));
                    	
                    	if (lat != 0 && lng != 0)
                    	{
                    		result.put(JsonObjects.SessionEvent.KEY_LATITUDE, lat);
                    		result.put(JsonObjects.SessionEvent.KEY_LONGITUDE, lng);
                    	}
                    }
                    
                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.CUST_ID)))
                    {
                        String customerID = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.CUST_ID));
                        String userType = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.USER_TYPE));

                        result.put(JsonObjects.SessionEvent.CUST_ID, customerID);
                        result.put(JsonObjects.SessionEvent.USER_TYPE, userType);
                    }

                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.IDENTIFIERS)))
                    {
                        String ids = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.IDENTIFIERS));
                        result.put(JsonObjects.SessionEvent.IDENTIFIERS, new JSONObject(ids));
                    }

                    /*
                     * Get the custom dimensions from the attributes table
                     */
                    Cursor attributesCursor = null;
                    try
                    {
                        attributesCursor = provider.query(AttributesDbColumns.TABLE_NAME, null, String.format("%s = ?", AttributesDbColumns.EVENTS_KEY_REF), new String[] { Long.toString(eventId) }, null); //$NON-NLS-1$

                        final int keyColumn = attributesCursor.getColumnIndexOrThrow(AttributesDbColumns.ATTRIBUTE_KEY);
                        final int valueColumn = attributesCursor.getColumnIndexOrThrow(AttributesDbColumns.ATTRIBUTE_VALUE);
                        while (attributesCursor.moveToNext())
                        {
                            final String key = attributesCursor.getString(keyColumn);
                            final String value = attributesCursor.getString(valueColumn);

                            if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_1, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_2, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_3, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_4, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_5, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_6, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_7, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_8, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_9, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_10, value);
                            }                                
                        }
                    }
                    finally
                    {
                        if (null != attributesCursor)
                        {
                            attributesCursor.close();
                            attributesCursor = null;
                        }
                    }
                }
                else if (Constants.OPT_IN_EVENT.equals(eventName) || Constants.OPT_OUT_EVENT.equals(eventName))
                {
                    result.put(JsonObjects.OptEvent.KEY_DATA_TYPE, JsonObjects.OptEvent.VALUE_DATA_TYPE);
                    result.put(JsonObjects.OptEvent.KEY_API_KEY, apiKey);
                    result.put(JsonObjects.OptEvent.KEY_OPT, Constants.OPT_OUT_EVENT.equals(eventName) ? Boolean.TRUE.toString() : Boolean.FALSE.toString());
                    result.put(JsonObjects.OptEvent.KEY_WALL_TIME_SECONDS, Math.round((double) cursor.getLong(cursor.getColumnIndex(EventsDbColumns.WALL_TIME))
                            / DateUtils.SECOND_IN_MILLIS));
                }
                else if (Constants.FLOW_EVENT.equals(eventName))
                {
                    result.put(JsonObjects.EventFlow.KEY_DATA_TYPE, JsonObjects.EventFlow.VALUE_DATA_TYPE);
                    result.put(JsonObjects.EventFlow.KEY_EVENT_UUID, cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.UUID)));
                    result.put(JsonObjects.EventFlow.KEY_SESSION_START_TIME, Math.round((double) sessionStartTime / DateUtils.SECOND_IN_MILLIS));

                    /*
                     * Need to generate two objects: the old flow events and the new flow events
                     */

                    /*
                     * Default sort order is ascending by _ID, so these will be sorted chronologically.
                     */
                    Cursor eventHistoryCursor = null;
                    try
                    {
                        eventHistoryCursor = provider.query(EventHistoryDbColumns.TABLE_NAME, new String[]
                            {
                                EventHistoryDbColumns.TYPE,
                                EventHistoryDbColumns.PROCESSED_IN_BLOB,
                                EventHistoryDbColumns.NAME }, String.format("%s = ? AND %s <= ?", EventHistoryDbColumns.SESSION_KEY_REF, EventHistoryDbColumns.PROCESSED_IN_BLOB), new String[] { Long.toString(sessionId), Long.toString(blobId) }, EventHistoryDbColumns._ID); //$NON-NLS-1$

                        final JSONArray newScreens = new JSONArray();
                        final JSONArray oldScreens = new JSONArray();
                        while (eventHistoryCursor.moveToNext())
                        {
                            final String name = eventHistoryCursor.getString(eventHistoryCursor.getColumnIndexOrThrow(EventHistoryDbColumns.NAME));
                            final String type;
                            if (EventHistoryDbColumns.TYPE_EVENT == eventHistoryCursor.getInt(eventHistoryCursor.getColumnIndexOrThrow(EventHistoryDbColumns.TYPE)))
                            {
                                type = JsonObjects.EventFlow.Element.TYPE_EVENT;
                            }
                            else
                            {
                                type = JsonObjects.EventFlow.Element.TYPE_SCREEN;
                            }

                            if (blobId == eventHistoryCursor.getLong(eventHistoryCursor.getColumnIndexOrThrow(EventHistoryDbColumns.PROCESSED_IN_BLOB)))
                            {
                                newScreens.put(new JSONObject().put(type, name));
                            }
                            else
                            {
                                oldScreens.put(new JSONObject().put(type, name));
                            }
                        }

                        result.put(JsonObjects.EventFlow.KEY_FLOW_NEW, newScreens);
                        result.put(JsonObjects.EventFlow.KEY_FLOW_OLD, oldScreens);
                    }
                    finally
                    {
                        if (null != eventHistoryCursor)
                        {
                            eventHistoryCursor.close();
                            eventHistoryCursor = null;
                        }
                    }
                }
                else
                {
                    /*
                     * This is a normal application event
                     */

                    result.put(JsonObjects.SessionEvent.KEY_DATA_TYPE, JsonObjects.SessionEvent.VALUE_DATA_TYPE);
                    result.put(JsonObjects.SessionEvent.KEY_WALL_TIME_SECONDS, Math.round((double) cursor.getLong(cursor.getColumnIndex(EventsDbColumns.WALL_TIME))
                            / DateUtils.SECOND_IN_MILLIS));
                    result.put(JsonObjects.SessionEvent.KEY_EVENT_UUID, cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.UUID)));
                    result.put(JsonObjects.SessionEvent.KEY_SESSION_UUID, sessionUuid);
                    result.put(JsonObjects.SessionEvent.KEY_NAME, eventName.substring(context.getPackageName().length() + 1, eventName.length()));

                    /*
                     * Add customer value increase if non-zero 
                     */                        
                    long clv = cursor.getLong(cursor.getColumnIndex(EventsDbColumns.CLV_INCREASE));
                    if (clv != 0)
                    {
                    	result.put(JsonObjects.SessionEvent.KEY_CUSTOMER_VALUE_INCREASE, clv);
                    }
                    
                    /*
                     * Append lat/lng if it is available
                     */
                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.LAT_NAME)) &&
                        false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.LNG_NAME)))
                    {
                    	double lat = cursor.getDouble(cursor.getColumnIndexOrThrow(EventsDbColumns.LAT_NAME));
                    	double lng = cursor.getDouble(cursor.getColumnIndexOrThrow(EventsDbColumns.LNG_NAME));
                    	
                    	if (lat != 0 && lng != 0)
                    	{
                    		result.put(JsonObjects.SessionEvent.KEY_LATITUDE, lat);
                    		result.put(JsonObjects.SessionEvent.KEY_LONGITUDE, lng);
                    	}
                    }
                    
                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.CUST_ID)))
                    {
                        String customerID = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.CUST_ID));
                        String userType = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.USER_TYPE));

                        result.put(JsonObjects.SessionEvent.CUST_ID, customerID);
                        result.put(JsonObjects.SessionEvent.USER_TYPE, userType);
                    }

                    if (false == cursor.isNull(cursor.getColumnIndexOrThrow(EventsDbColumns.IDENTIFIERS)))
                    {
                        String ids = cursor.getString(cursor.getColumnIndexOrThrow(EventsDbColumns.IDENTIFIERS));
                        result.put(JsonObjects.SessionEvent.IDENTIFIERS, new JSONObject(ids));
                    }

                    /*
                     * Get the custom dimensions from the attributes table
                     */
                    Cursor attributesCursor = null;
                    try
                    {
                        attributesCursor = provider.query(AttributesDbColumns.TABLE_NAME, null, String.format("%s = ?", AttributesDbColumns.EVENTS_KEY_REF), new String[] { Long.toString(eventId) }, null); //$NON-NLS-1$

                        final int keyColumn = attributesCursor.getColumnIndexOrThrow(AttributesDbColumns.ATTRIBUTE_KEY);
                        final int valueColumn = attributesCursor.getColumnIndexOrThrow(AttributesDbColumns.ATTRIBUTE_VALUE);
                        while (attributesCursor.moveToNext())
                        {
                            final String key = attributesCursor.getString(keyColumn);
                            final String value = attributesCursor.getString(valueColumn);

                            if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_1, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_2, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_3, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_4, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_5, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_6, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_7, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_8, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_9, value);
                            }
                            else if (AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10.equals(key))
                            {
                                result.put(JsonObjects.SessionOpen.KEY_CUSTOM_DIMENSION_10, value);
                            }
                        }
                    }
                    finally
                    {
                        if (null != attributesCursor)
                        {
                            attributesCursor.close();
                            attributesCursor = null;
                        }
                    }

                    final JSONObject attributes = convertAttributesToJson(provider, context, eventId);

                    if (null != attributes)
                    {
                        result.put(JsonObjects.SessionEvent.KEY_ATTRIBUTES, attributes);
                    }
                }
            }
            else
            {
                /*
                 * This should never happen
                 */
                throw new RuntimeException();
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

        return result;
    }

    /**
     * Private helper to get the {@link SessionsDbColumns#_ID} for a given {@link EventsDbColumns#_ID}.
     *
     * @param provider Localytics database instance. Cannot be null.
     * @param eventId {@link EventsDbColumns#_ID} of the event to look up
     * @return The {@link SessionsDbColumns#_ID} of the session that owns the event.
     */
    /* package */static long getSessionIdForEventId(final LocalyticsProvider provider, final long eventId)
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(EventsDbColumns.TABLE_NAME, new String[]
                { EventsDbColumns.SESSION_KEY_REF }, String.format("%s = ?", EventsDbColumns._ID), new String[] { Long.toString(eventId) }, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
                return cursor.getLong(cursor.getColumnIndexOrThrow(EventsDbColumns.SESSION_KEY_REF));
            }

            /*
             * This should never happen
             */
            throw new RuntimeException();
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
     * Private helper to get the {@link SessionsDbColumns#UUID} for a given {@link SessionsDbColumns#_ID}.
     *
     * @param provider Localytics database instance. Cannot be null.
     * @param sessionId {@link SessionsDbColumns#_ID} of the event to look up
     * @return The {@link SessionsDbColumns#UUID} of the session.
     */
    /* package */static String getSessionUuid(final LocalyticsProvider provider, final long sessionId)
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(SessionsDbColumns.TABLE_NAME, new String[]
                { SessionsDbColumns.UUID }, String.format("%s = ?", SessionsDbColumns._ID), new String[] { Long.toString(sessionId) }, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
                return cursor.getString(cursor.getColumnIndexOrThrow(SessionsDbColumns.UUID));
            }

            /*
             * This should never happen
             */
            throw new RuntimeException();
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
     * Private helper to get a column value from the InfoDb table
     *
     * @param provider Localytics database provider. Cannot be null.
     * @param key Database key. Cannot be null.
     * @return The requested string
     */
    /* package */static String getStringFromAppInfo(final LocalyticsProvider provider, final String key)
    {
        Cursor cursor = null;
        
        try
        {
            cursor = provider.query(InfoDbColumns.TABLE_NAME, null, null, null, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
            	return cursor.getString(cursor.getColumnIndexOrThrow(key));
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
     * Private helper to get the {@link SessionsDbColumns#SESSION_START_WALL_TIME} for a given {@link SessionsDbColumns#_ID}.
     *
     * @param provider Localytics database instance. Cannot be null.
     * @param sessionId {@link SessionsDbColumns#_ID} of the event to look up
     * @return The {@link SessionsDbColumns#SESSION_START_WALL_TIME} of the session.
     */
    /* package */static long getSessionStartTime(final LocalyticsProvider provider, final long sessionId)
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(SessionsDbColumns.TABLE_NAME, new String[]
                { SessionsDbColumns.SESSION_START_WALL_TIME }, String.format("%s = ?", SessionsDbColumns._ID), new String[] { Long.toString(sessionId) }, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
                return cursor.getLong(cursor.getColumnIndexOrThrow(SessionsDbColumns.SESSION_START_WALL_TIME));
            }

            /*
             * This should never happen
             */
            throw new RuntimeException();
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
     * Private helper to get the {@link SessionsDbColumns#ELAPSED_TIME_SINCE_LAST_SESSION} for a given {@link SessionsDbColumns#_ID}.
     *
     * @param provider Localytics database instance. Cannot be null.
     * @param sessionId {@link SessionsDbColumns#_ID} of the event to look up
     * @return The {@link SessionsDbColumns#ELAPSED_TIME_SINCE_LAST_SESSION} of the session.
     */
    /* package */static long getElapsedTimeSinceLastSession(final LocalyticsProvider provider, final long sessionId)
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(SessionsDbColumns.TABLE_NAME, new String[]
                    { SessionsDbColumns.ELAPSED_TIME_SINCE_LAST_SESSION }, String.format("%s = ?", SessionsDbColumns._ID), new String[] { Long.toString(sessionId) }, null); //$NON-NLS-1$

            if (cursor.moveToFirst())
            {
                return cursor.getLong(cursor.getColumnIndexOrThrow(SessionsDbColumns.ELAPSED_TIME_SINCE_LAST_SESSION));
            }

            /*
             * This should never happen
             */
            throw new RuntimeException();
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
     * Private helper to convert an event's attributes into a {@link JSONObject} representation.
     *
     * @param provider Localytics database instance. Cannot be null.
     * @param context Application context. Cannot be null.
     * @param eventId {@link EventsDbColumns#_ID} of the event whose attributes are to be loaded.
     * @return {@link JSONObject} representing the attributes of the event. The order of attributes is undefined and may
     *         change from call to call of this method. If the event has no attributes, returns null.
     * @throws JSONException if an error occurs converting the attributes to JSON
     */
    /* package */static JSONObject convertAttributesToJson(final LocalyticsProvider provider, final Context context, final long eventId) throws JSONException
    {
        Cursor cursor = null;
        try
        {
            cursor = provider.query(AttributesDbColumns.TABLE_NAME, null, String.format("%s = ? AND %s != ? AND %s != ? AND %s != ? AND %s != ? AND %s != ? AND %s != ? AND %s != ? AND %s != ? AND %s != ? AND %s != ?", AttributesDbColumns.EVENTS_KEY_REF, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY, AttributesDbColumns.ATTRIBUTE_KEY), new String[] { Long.toString(eventId), AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_1, AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_2, AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_3,  AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_4, AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_5, AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_6, AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_7, AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_8, AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_9, AttributesDbColumns.ATTRIBUTE_CUSTOM_DIMENSION_10 }, null); //$NON-NLS-1$

            if (cursor.getCount() == 0)
            {
                return null;
            }

            final JSONObject attributes = new JSONObject();

            final int keyColumn = cursor.getColumnIndexOrThrow(AttributesDbColumns.ATTRIBUTE_KEY);
            final int valueColumn = cursor.getColumnIndexOrThrow(AttributesDbColumns.ATTRIBUTE_VALUE);
            while (cursor.moveToNext())
            {
                final String key = cursor.getString(keyColumn);
                final String value = cursor.getString(valueColumn);

                attributes.put(key.substring(context.getPackageName().length() + 1, key.length()), value);
            }

            return attributes;
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
     * Given an id of an upload blob, get the session id associated with that blob.
     *
     * @param blobId {@link UploadBlobsDbColumns#_ID} of the upload blob.
     * @return id of the parent session.
     */
    /* package */static long getSessionIdForBlobId(final LocalyticsProvider provider, final long blobId)
    {
        /*
         * This implementation needs to walk up the tree of database elements.
         */

        long eventId;
        {
            Cursor cursor = null;
            try
            {
                cursor = provider.query(UploadBlobEventsDbColumns.TABLE_NAME, new String[]
                    { UploadBlobEventsDbColumns.EVENTS_KEY_REF }, String.format("%s = ?", UploadBlobEventsDbColumns.UPLOAD_BLOBS_KEY_REF), new String[] //$NON-NLS-1$
                    { Long.toString(blobId) }, null);

                if (cursor.moveToFirst())
                {
                    eventId = cursor.getLong(cursor.getColumnIndexOrThrow(UploadBlobEventsDbColumns.EVENTS_KEY_REF));
                }
                else
                {
                    /*
                     * This should never happen
                     */
                    throw new RuntimeException("No events associated with blob"); //$NON-NLS-1$
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

        long sessionId;
        {
            Cursor cursor = null;
            try
            {
                cursor = provider.query(EventsDbColumns.TABLE_NAME, new String[]
                    { EventsDbColumns.SESSION_KEY_REF }, String.format("%s = ?", EventsDbColumns._ID), new String[] //$NON-NLS-1$
                    { Long.toString(eventId) }, null);

                if (cursor.moveToFirst())
                {
                    sessionId = cursor.getLong(cursor.getColumnIndexOrThrow(EventsDbColumns.SESSION_KEY_REF));
                }
                else
                {
                    /*
                     * This should never happen
                     */
                    throw new RuntimeException("No session associated with event"); //$NON-NLS-1$
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

        return sessionId;
    }
}
