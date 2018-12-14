package recommender.content_based.demo;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArtistInfo {
    public List<TagInfo> tags = new ArrayList<>();
    public String id;
    public String name;
    public String url;
    public String pictureURL;

    public void initTag(String line) {
        String temp = StringUtils.substringAfter(line, ",");
        String tag = StringUtils.substringBefore(temp, ")");
        temp = StringUtils.substringAfter(temp, "\t");
        Double score = Double.parseDouble(temp);

        TagInfo tagInfo = new TagInfo();
        tagInfo.tag = tag;
        tagInfo.score = score;
        tags.add(tagInfo);
    }

    public void sortTags() {
        Collections.sort(tags);
    }
}
