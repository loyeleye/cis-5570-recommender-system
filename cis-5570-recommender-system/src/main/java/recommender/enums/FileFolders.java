package recommender.enums;

public enum FileFolders {
    INPUT("input"),
    IP_PC("itemProfilePC"),
    IP_TW("itemProfile"),
    UP_PC("userProfilePC"),
    UP_NN("userProfNonNorm"),
    UP_TW("userProfile"),
    UP_JN("userProfJoin"),
    UA_CT("userArtistCount"),
    INDEX("invertedIndex"),
    VEC_PR("vectorProfile"),
    COSSIM("cosineSimilarity");

    private final String foldername;

    FileFolders(String name) {
        foldername = name;
    }

    public String foldername() {
        return foldername;
    }
}
