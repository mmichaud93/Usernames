package com.io.usernames;

import android.content.Context;
import android.os.Bundle;
import android.support.v7.app.ActionBar;
import android.support.v7.app.ActionBarActivity;
import android.view.LayoutInflater;
import android.view.View;

import de.keyboardsurfer.android.widget.crouton.Crouton;


public class UsernamesActivity extends ActionBarActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
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
}
