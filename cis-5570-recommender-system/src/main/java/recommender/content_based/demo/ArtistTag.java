package recommender.content_based.demo;

public class ArtistTag {
        public String artist;
        public String tag;

        ArtistTag(String artist, String tag) {
            this.artist = artist;
            this.tag = tag;
        }
        
        public static ArtistTag create(String a, String t) {
            return new ArtistTag(a, t);
        }
    }