package recommender.content_based.demo;

import org.apache.commons.lang.StringUtils;

import java.util.List;

public class OldArtistInfo extends ArtistInfo implements Comparable<OldArtistInfo> {
    public Integer playcount;

    public static OldArtistInfo init(String line) {
        String[] oai = StringUtils.split(line, '\t');
        OldArtistInfo oldArtistInfo = new OldArtistInfo();
        oldArtistInfo.id = oai[1];
        oldArtistInfo.playcount = Integer.parseInt(oai[2]);

        return oldArtistInfo;
    }

    @Override
    public int compareTo(OldArtistInfo o) {
        int cmp = playcount.compareTo(o.playcount) * -1;
        return (cmp == 0) ? id.compareTo(o.id) : cmp;
    }

    public String print() {
        String s = String.format("Artist ID: %s\nName: %s\nUrl: %s\nPicture Url: %s\nPlaycount: %s\n",
                id, name, url, pictureURL, playcount);
        sortTags();
        return s + printTags();
    }

    public String printTags() {
        int maxTags = 10;
        StringBuilder ret = new StringBuilder("Tags related to this artist:\n");
        for (TagInfo tag: tags) {
            if (maxTags-- <= 0) break;
            ret.append('\t');
            ret.append(String.format("(%s) %s >>> Tag Weight: %f", tag.tag, tag.tagName, tag.score));
            ret.append('\n');
        }
        return ret.toString();
    }
}
