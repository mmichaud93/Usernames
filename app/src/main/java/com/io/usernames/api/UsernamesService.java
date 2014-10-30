package com.io.usernames.api;

import com.io.usernames.models.UsernameModel;

import retrofit.Callback;
import retrofit.http.GET;

/**
 * Created by matthewmichaud on 10/23/14.
 */
public interface UsernamesService {
    @GET("/username")
    void getUsername(Callback<UsernameModel> callback);
}
