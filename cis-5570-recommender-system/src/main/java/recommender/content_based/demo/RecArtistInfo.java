package recommender.content_based.demo;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class RecArtistInfo extends ArtistInfo implements Comparable<RecArtistInfo> {
    public Double recommendationScore;

    @Override
    public int compareTo(RecArtistInfo o) {
        int cmp = recommendationScore.compareTo(o.recommendationScore) * -1;
        return (cmp == 0) ? id.compareTo(o.id) : cmp;
    }

    public String print() {
        int maxTags = 10;
        String s = String.format("Artist ID: %s\nName: %s\nUrl: %s\nPicture Url: %s\nRecommendation Score: %s\n",
                id, name, url, pictureURL, recommendationScore);
        StringBuilder ret = new StringBuilder(s);
        sortTags();
        ret.append("Tags used to calculate the recommendation score:\n");
        for (TagInfo tag: tags) {
            if (tag.contribution <= 0 || maxTags-- <= 0) break;
            ret.append(print(tag));
            ret.append('\n');
        }
        return ret.toString();
    }

    public static RecArtistInfo init(String line) {
        String temp = StringUtils.substringAfter(line, "Artist ");
        temp = StringUtils.substringBefore(temp, " to User");
        String rec = StringUtils.substringAfterLast(line, "\t");
        Double score = Double.parseDouble(rec);
        RecArtistInfo artistInfo = new RecArtistInfo();
        artistInfo.id = temp;
        artistInfo.recommendationScore = score;

        return artistInfo;
    }

    public String print(TagInfo tagInfo) {
        return String.format("\t(%s) %s >>> Contribution to Recommendation: %f%%, Weight in Artist Profile: ", tagInfo.tag, tagInfo.tagName, tagInfo.contribution * 100, tagInfo.score);
    }
}
