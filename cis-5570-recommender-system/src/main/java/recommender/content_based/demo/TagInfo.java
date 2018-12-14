package recommender.content_based.demo;

import org.apache.commons.lang.StringUtils;

public class TagInfo implements Comparable<TagInfo> {
    public String tag;
    public String tagName;
    public Double score = 0d;
    public Double contribution = 0d;

    @Override
    public int compareTo(TagInfo o) {
        int cmp = contribution.compareTo(o.contribution) * -1;
        cmp = (cmp == 0) ? score.compareTo(o.score) * -1 : cmp;
        return (cmp == 0) ? tag.compareTo(o.tag) : cmp;
    }

    public void setName(String line) {
        tagName = StringUtils.split(line, '\t')[1];
    }

}
