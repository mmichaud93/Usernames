package com.io.usernames;

import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.support.v4.view.PagerAdapter;
import android.text.SpannableString;
import android.text.style.UnderlineSpan;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.io.usernames.models.UsernameModel;

import java.util.List;

import butterknife.ButterKnife;
import butterknife.InjectView;

/**
 * Created by michaudm3 on 10/23/2014.
 */
public class UsernamesAdapter extends PagerAdapter {

    private final String TAG = "UsernamesAdapter";

    Context context;
    private int resource;
    private List<UsernameModel> items;

    public UsernamesAdapter(Context context, int resource, List<UsernameModel> items) {
        this.context = context;
        this.items = items;
        this.resource = resource;
    }

    public List<UsernameModel> getItems() {
        return items;
    }

    public void setItems(List<UsernameModel> items) {
        this.items = items;
    }

    @Override
    public Object instantiateItem(ViewGroup container, int position) {
        LayoutInflater inflater = (LayoutInflater) context
                .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        View root=null;
        final UsernameModel item = items.get(position);
        if(item!=null) {
            root = inflater.inflate(resource, container, false);
            TextView textView = (TextView) root.findViewById(R.id.text_username);
            textView.setText(item.getUsername());

            /*
             *  URL
             */
            ImageView urlStatus = (ImageView) root.findViewById(R.id.url_status);
            TextView urlText = (TextView) root.findViewById(R.id.url_text);
            SpannableString urlContent = new SpannableString(item.getUsername()+".com");
            urlContent.setSpan(new UnderlineSpan(), 0, urlContent.length(), 0);
            urlText.setText(urlContent);
            urlText.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    launchLink(v, "http://www.namecheap.com/?aff=72210");
                }
            });

            if(item.getResults().get(0).isAvailable()) {
                urlStatus.setImageResource(R.drawable.ic_check);
            } else {
                urlStatus.setImageResource(R.drawable.ic_x);
            }

            /*
             *  FACEBOOK
             */
            ImageView facebookStatus = (ImageView) root.findViewById(R.id.facebook_status);
            TextView facebookText = (TextView) root.findViewById(R.id.facebook_text);
            SpannableString facebookContent = new SpannableString("facebook.com/"+item.getUsername());
            facebookContent.setSpan(new UnderlineSpan(), 0, facebookContent.length(), 0);
            facebookText.setText(facebookContent);
            facebookText.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    launchLink(v, "https://facebook.com/username");
                }
            });

            if(item.getResults().get(0).isAvailable()) {
                facebookStatus.setImageResource(R.drawable.ic_check);
            } else {
                facebookStatus.setImageResource(R.drawable.ic_x);
            }

            /*
             *  TWITTER
             */
            ImageView twitterStatus = (ImageView) root.findViewById(R.id.twitter_status);
            TextView twitterText = (TextView) root.findViewById(R.id.twitter_text);
            SpannableString twitterContent = new SpannableString("twitter.com/"+item.getUsername());
            twitterContent.setSpan(new UnderlineSpan(), 0, twitterContent.length(), 0);
            twitterText.setText(twitterContent);
            twitterText.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    launchLink(v, "https://twitter.com/signup");
                }
            });

            if(item.getResults().get(0).isAvailable()) {
                twitterStatus.setImageResource(R.drawable.ic_check);
            } else {
                twitterStatus.setImageResource(R.drawable.ic_x);
            }

            /*
             *  GITHUB
             */
            ImageView githubStatus = (ImageView) root.findViewById(R.id.github_status);
            TextView githubText = (TextView) root.findViewById(R.id.github_text);
            SpannableString githubContent = new SpannableString("github.com/"+item.getUsername());
            githubContent.setSpan(new UnderlineSpan(), 0, githubContent.length(), 0);
            githubText.setText(githubContent);
            githubText.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    launchLink(v, "https://github.com/join");
                }
            });

            if(item.getResults().get(0).isAvailable()) {
                githubStatus.setImageResource(R.drawable.ic_check);
            } else {
                githubStatus.setImageResource(R.drawable.ic_x);
            }

            /*
             *  LINKEDIN
             */
            ImageView linkedinStatus = (ImageView) root.findViewById(R.id.linkedin_status);
            TextView linkedinText = (TextView) root.findViewById(R.id.linkedin_text);
            SpannableString linkedinContent = new SpannableString("linkedin.com/"+item.getUsername());
            linkedinContent.setSpan(new UnderlineSpan(), 0, linkedinContent.length(), 0);
            linkedinText.setText(linkedinContent);
            linkedinText.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    launchLink(v, "http://www.linkedin.com/profile/public-profile-settings");
                }
            });

            if(item.getResults().get(0).isAvailable()) {
                linkedinStatus.setImageResource(R.drawable.ic_check);
            } else {
                linkedinStatus.setImageResource(R.drawable.ic_x);
            }

            container.addView(root);
        }
        return root;
    }

    @Override
    public void destroyItem(ViewGroup collection, int position, Object view) {
        (collection).removeView((View) view);
    }

    @Override
    public int getCount() {
        return items.size();
    }

    @Override
    public boolean isViewFromObject(View view, Object object) {
        return view==object;
    }

    public void launchLink(View view, String url) {
        Intent intent = new Intent(Intent.ACTION_VIEW);
        intent.setData(Uri.parse(url));
        view.getContext().startActivity(intent);
    }

    public void addUsername(UsernameModel usernameModel) {
        items.add(usernameModel);
        notifyDataSetChanged();
    }

    public void removeUsername(int i) {
        items.remove(i);
        notifyDataSetChanged();
    }

    public UsernameModel getUsername(int i) {
        return items.get(i);
    }
}
