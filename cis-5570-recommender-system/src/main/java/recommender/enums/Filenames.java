package recommender.enums;

public enum Filenames {
    A("artists"),
    T("tags"),
    UA("user_artists"),
    UF("user_friends"),
    UT("user_taggedartists"),
    UTT("user_taggedartists-timestamps");

    private final String filename;

    Filenames(String name) {
        filename = name;
    }

    public String filename() {
        return filename;
    }
}
