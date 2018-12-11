package recommender.deprecated;

import org.apache.hadoop.io.DoubleWritable;
import recommender.hadoopext.io.ProfileFeatureWritable;
import recommender.hadoopext.io.ProfileIdWritable;

/**
 * Support class to make it easier to extract values of a feature
 */
public class Feature {
    private boolean isUser;
    private int id;
    private String featureId;
    private int tagId;
    private boolean isTagFeature;
    private double featureValue;

    private ProfileFeatureWritable key;
    private DoubleWritable value;

    public Feature(Boolean isUser, Integer id, String featureId, Boolean isTagFeature, Double featureValue) {
        this.isUser = isUser;
        this.id = id;
        this.featureId = featureId;
        this.isTagFeature = isTagFeature;
        this.featureValue = featureValue;
        this.tagId = (isTagFeature) ? Integer.parseInt(featureId) : 0;

        ProfileIdWritable profileIdWritable = new ProfileIdWritable(isUser, id);
        key = new ProfileFeatureWritable(profileIdWritable, featureId);
        value = new DoubleWritable(featureValue);
    }

    public Feature() {}

    /**
     * Returns a key value identifying this feature's properties. Is of the form (<user/artist>-<id>,<feature>).
     * @return A key that can be used as a key in a map reduce job.
     */
    public ProfileFeatureWritable getKey() {
        return key;
    }

    /**
     * Returns a value for this feature. For tag, it is a percentage score. For playcount, it is the average
     * playcount for the user or artist. A user's average playcount is the average # of plays a user has made
     * across all artists (s)he has listened to. An artist's average playcount is the average # of times a
     * user has played this artist averaging all users who have listened to this artist at least once.
     * @return The feature's value that can be used as a value in a map reduce job.
     */
    public DoubleWritable getValue() {
        return value;
    }

    /**
     * Check if the feature type is for a "tag". Also does a print to console for debugging.
     * @return true if the feature type is a tag percentage
     */
    public boolean isTag() {
        System.out.printf("%s is a tag: %s", this.toString(), isTagFeature);
        return isTagFeature;
    }

    /**
     * Check if the feature type is "playcount". Also does a print to console for debugging purposes.
     * @return true if the feature type is playcount
     */
    public boolean isPlaycount() {
        System.out.printf("%s is a playcount: %s", this.toString(), !isTagFeature);
        return !isTagFeature;
    }

    /**
     * Check if this is a user profile feature. Also does a print to console for debugging purposes.
     * @return true if this is a user profile feature
     */
    public boolean ofUser() {
        System.out.printf("%s is a user profile feature: %s", this.toString(), !isUser);
        return isUser;
    }

    /**
     * Check if this is an artist profile feature. Also does a print to console for debugging purposes.
     * @return true if this is an artist profile feature
     */
    public boolean ofArtist() {
        System.out.printf("%s is an item profile feature: %s", this.toString(), isUser);
        return !isUser;
    }

    /**
     * Get the user id this feature belongs to.
     * @return The user id (e.g. in the userProfile files if the user is "user-1", the user id is 1)
     * @throws Exception If this is not a user profile
     */
    public int getUserId() throws Exception {
        if (!isUser) throw new Exception("ERROR: This feature is not for a User Profile! Try getArtistId() instead!");

        return id;
    }

    /**
     * Get the artist id this feature belongs to.
     * @return The artist id (e.g. in the itemProfile files if the artist is "artist-1", the artist id is 1)
     * @throws Exception If this is not an artist profile
     */
    public int getArtistId() throws Exception {
        if (isUser) throw new Exception("ERROR: This feature is not for an Artist Profile! Try getUserId() instead!");

        return id;
    }

    public int getTagId() throws Exception {
        if (!isTagFeature) throw new Exception("ERROR: This feature is not a tag! Try getPlaycount() instead!");

        return tagId;
    }

    /**
     * Get the percentage score for how much this tag influences this user or artist profile preference
     * @return The tag percentage score - if it is a tag feature
     */
    public double getTagPercentage() throws Exception {
        if (!isTagFeature) throw new Exception("ERROR: This feature is not a tag! Try getPlaycount() instead!");

        return featureValue;
    }

    /**
     * Get the average playcount for this user or artist profile. For a user, this means average plays across all artists.
     * For an artist, this means average plays across all users.
     * @return The average playcount - if it is a weight/playcount feature
     * @throws Exception
     */
    public double getPlaycount() throws Exception {
        if (isTagFeature) throw new Exception("ERROR: This feature is not a playcount! Try getTagPercentage() instead!");

        return featureValue;
    }

    @Override
    public String toString() {
        return String.format("[(%s-%d,%s)  %f]", isUser ? "user" : "artist", id, isTagFeature ? "tag-" + featureId : "playcount", featureValue);
    }
}
