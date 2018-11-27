package recommender.enums;

public enum Filenames {
    A("artists.dat"),
    T("tags.dat"),
    UA("user_artists.dat"),
    UF("user_friends.dat"),
    UT("user_taggedartists.dat"),
    UTT("user_taggedartists-timestamps.dat");

    private final String filename;

    Filenames(String name) {
        filename = name;
    }

    public String filename() {
        return filename;
    }
}
