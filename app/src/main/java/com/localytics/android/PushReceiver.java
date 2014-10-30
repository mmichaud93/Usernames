//@formatter:off
/**
 * PushReceiver.java Copyright (C) 2013 Char Software Inc., DBA Localytics. This code is provided under the Localytics Modified
 * BSD License. A copy of this license has been distributed in a file called LICENSE with this source code. Please visit
 * www.localytics.com for more information.
 */
//@formatter:on

package com.localytics.android;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;

public class PushReceiver extends BroadcastReceiver
{
	public void onReceive(Context context, Intent intent) 
	{
        LocalyticsSession session = new LocalyticsSession(context);
		// Registration complete
		if (intent.getAction().equals("com.google.android.c2dm.intent.REGISTRATION")) 
		{
            session.handleRegistration(intent);
		}
		// Notification received
		else if (intent.getAction().equals("com.google.android.c2dm.intent.RECEIVE")) 
		{
            session.handleNotificationReceived(intent);
		}
	}
}
