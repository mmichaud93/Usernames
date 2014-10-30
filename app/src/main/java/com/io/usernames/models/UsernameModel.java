package com.io.usernames.models;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Created by matthewmichaud on 10/23/14.
 */
public class UsernameModel {
    @Expose
    @SerializedName("username")
    String username;
    @Expose
    @SerializedName("results")
    List<ServiceModel> results;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public List<ServiceModel> getResults() {
        return results;
    }

    public void setResults(List<ServiceModel> results) {
        this.results = results;
    }

    public class ServiceModel {
        @Expose
        @SerializedName("available")
        boolean available;

        @Expose
        @SerializedName("service")
        String service;

        public boolean isAvailable() {
            return available;
        }

        public void setAvailable(boolean available) {
            this.available = available;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }
    }
}
