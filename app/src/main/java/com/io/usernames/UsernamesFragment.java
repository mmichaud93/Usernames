package com.io.usernames;

import android.annotation.SuppressLint;
import android.os.Bundle;
import android.os.Handler;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ProgressBar;

import com.io.usernames.api.UsernamesService;
import com.io.usernames.models.UsernameModel;
import com.io.usernames.ui.CustomViewPager.ViewPagerCustomDuration;
import com.io.usernames.util.UsernameLog;
import com.io.usernames.util.Utility;

import java.util.ArrayList;
import java.util.List;

import butterknife.ButterKnife;
import butterknife.InjectView;
import de.keyboardsurfer.android.widget.crouton.Configuration;
import de.keyboardsurfer.android.widget.crouton.Crouton;
import de.keyboardsurfer.android.widget.crouton.Style;
import retrofit.Callback;
import retrofit.RequestInterceptor;
import retrofit.RestAdapter;
import retrofit.RetrofitError;
import retrofit.client.Response;

/**
 * Created by matthewmichaud on 10/23/14.
 */
public class UsernamesFragment extends Fragment {

    private final String TAG = "UsernamesFragment";

    UsernamesService usernamesService;

    @InjectView(R.id.username_tryanother)
    Button tryAnother;
    @InjectView(R.id.viewpager_username)
    ViewPagerCustomDuration viewPager;
    @InjectView(R.id.loading)
    ProgressBar loading;
    UsernamesAdapter usernameAdapter;

    List<UsernameModel> usernames;

    Style customStyle;
    Configuration customConfig;

    public UsernamesFragment() {
        RequestInterceptor requestInterceptor = new RequestInterceptor() {
            @Override
            public void intercept(RequestFacade request) {
                request.addHeader("User-Agent", "Usernames Android App");
            }
        };

        RestAdapter restAdapter = new RestAdapter.Builder()
                .setEndpoint("http://www.usernames.io")
                .setRequestInterceptor(requestInterceptor)
                .build();

        usernamesService = restAdapter.create(UsernamesService.class);

        usernames = new ArrayList<UsernameModel>();
        usernames.add(null);
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        View rootView = inflater.inflate(R.layout.fragment_usernames, container, false);
        ButterKnife.inject(this, rootView);

        tryAnother.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                UsernamesActivity.tagEvent(UsernamesActivity.TRY_ANOTHER_EVENT);
                if(usernames.size()==1 && usernames.get(0)==null) {
                    if(loading.getVisibility()==View.GONE) {
                        loading.setVisibility(View.VISIBLE);
                    }
                }
                getUsername();
            }
        });

        usernameAdapter = new UsernamesAdapter(getActivity(), R.layout.adapter_username, usernames);
        viewPager.setAdapter(usernameAdapter);
        getUsername();

        return rootView;
    }

    @SuppressLint("ResourceAsColor")
    @Override
    public void onActivityCreated(@Nullable Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);

        int heightInPx = Utility.dpToPx(getActivity(), 32);

        customStyle = new Style.Builder()
                .setBackgroundColor(R.color.app_primary_dark)
                .setTextColor(R.color.app_accent)
                .setHeight(heightInPx)
                .build();

        customConfig = new Configuration.Builder()
                .setDuration(3000)
                .build();
    }

    public void getUsername() {
        usernamesService.getUsername(usernameModelCallback);
    }

    Callback<UsernameModel> usernameModelCallback = new Callback<UsernameModel>() {

        @Override
        public void success(UsernameModel usernameModel, Response response) {
            if(getActivity()==null) {
                return;
            }
            if(usernameModel!=null) {
                UsernamesActivity.tagEvent(UsernamesActivity.USERNAME_EVENT);
                usernameAdapter.addUsername(usernameModel);
                if(usernameAdapter.getCount()<2) {
                    getUsername();
                } else {
                    viewPager.setCurrentItem(usernameAdapter.getCount()+1);
                }
                if(usernameAdapter.getCount()>2 && usernameAdapter.getUsername(0)==null) {
                    usernameAdapter.removeUsername(0);
                    viewPager.setAdapter(usernameAdapter);
                    // Hack to remove the first null username object
                    Handler handler = new Handler();
                    handler.postDelayed(new Runnable() {
                        @Override
                        public void run() {
                            getActivity().runOnUiThread(new Runnable() {
                                @Override
                                public void run() {
                                    viewPager.setCurrentItem(usernameAdapter.getCount()+1);
                                }
                            });
                        }
                    }, 500);
                }
                if(loading.getVisibility()!=View.GONE) {
                    loading.setVisibility(View.GONE);
                }
            }
        }

        @Override
        public void failure(RetrofitError error) {
            UsernameLog.e(TAG, error.getMessage());
            Crouton.makeText(
                    getActivity(),
                    getResources().getString(R.string.network_error),
                    customStyle).
                    setConfiguration(customConfig).
                    show();
            if(loading.getVisibility()!=View.GONE) {
                loading.setVisibility(View.GONE);
            }
        }
    };
}