package recommender.content_based.demo;

import org.apache.commons.lang.StringUtils;
import recommender.content_based.Main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Demo {
    private static boolean VERBOSE = true;

    public static void main( String[] args) throws Exception {
        if (args.length == 0 || !args[0].equalsIgnoreCase("-norec")) {
            //Main.main(args);

            System.out.println("Finished generating recommendations.");
            bigPause();
        }

        boolean analyzing = true;


        while (analyzing) {
            analyzeUser();
            analyzing = userConfirm("Would you like to explore more recommendations?");
            if (analyzing) VERBOSE = !userConfirm("Turn verbose mode off?");
        }

        System.out.println("Thanks for checking out our demo. Press any key to exit.");
        System.in.read();
    }

    private static void analyzeUser() throws IOException {
        System.out.println("Enter User ID to analyze: ");
        List<OldArtistInfo> oldArtistInfos = new ArrayList<>();
        List<RecArtistInfo> recArtistInfos = new ArrayList<>();
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        String in = input.readLine();
        int userId = Integer.parseInt(in);
        String filename = "output/recommendations/"+userId+"-r-00000";
        BufferedReader recs = Files.newBufferedReader(Paths.get(filename), Charset.defaultCharset());
        if (VERBOSE) System.out.println("Opened " + filename + "...");
        if (VERBOSE) System.out.println("Reading recommendations: ");
        String line;
        String user = String.valueOf(userId);
        while ((line = recs.readLine()) != null) {
            if (VERBOSE) System.out.println(line);
            recArtistInfos.add(RecArtistInfo.init(line));
        }
        recs.close();

        System.out.println("Finding artist names of recommended artists...");
        recArtistInfos = findArtists(recArtistInfos);

        BufferedReader uaReader = openReader("input/user_artists.dat");
        List<String> listenedToArtists = new ArrayList<>();
        if (VERBOSE) System.out.println(String.format("Finding artists previously listened to by User %s: ", userId));
        while ((line = uaReader.readLine()) != null) {
            if (StringUtils.startsWith(line, userId + "\t")) {
                if (VERBOSE) System.out.println(line);
                oldArtistInfos.add(OldArtistInfo.init(line));
            }
        }
        uaReader.close();

        System.out.println("Finding artist names of listened to artists...");
        oldArtistInfos = findArtists(oldArtistInfos);

        System.out.println("Generating user/item profiles...");

        BufferedReader userProfReader = openReader("userProfile/part-r-00000");
        HashMap<String, Double> userProfileTags = new HashMap<>();
        while ((line = userProfReader.readLine()) != null) {
            if (StringUtils.contains(line, "user-" + userId + ",")) {
                if (VERBOSE) System.out.println(line);
                String temp = StringUtils.substringAfter(line, ",");
                String tag = StringUtils.substringBefore(temp, ")");
                temp = StringUtils.substringAfter(temp, "\t");
                Double score = Double.parseDouble(temp);
                userProfileTags.put(tag, score);
            }
        }
        userProfReader.close();

        BufferedReader itemProfReader = openReader("itemProfile/part-r-00000");
        while ((line = itemProfReader.readLine()) != null) {
            for (RecArtistInfo artist: recArtistInfos) {
                String artistId = artist.id;
                if (StringUtils.contains(line, "artist-" + artistId + ",")) {
                    if (VERBOSE) System.out.println(line);
                    artist.initTag(line);
                }
            }
            for (OldArtistInfo artist: oldArtistInfos) {
                String artistId = artist.id;
                if (StringUtils.contains(line, "artist-" + artistId + ",")) {
                    if (VERBOSE) System.out.println(line);
                    artist.initTag(line);
                }
            }
        }
        itemProfReader.close();

        System.out.println("Calculating tag contributions...");
        for (RecArtistInfo recArtistInfo: recArtistInfos) {
            for (TagInfo tagInfo: recArtistInfo.tags) {
                tagInfo.contribution = tagInfo.score * getOrDefault(userProfileTags, tagInfo.tag, 0d) / recArtistInfo.recommendationScore;
            }
        }

        System.out.println("Reading user tags...");
        BufferedReader tagReader = openReader("data/tags.dat");
        while ((line = tagReader.readLine()) != null) {
            for (RecArtistInfo artist: recArtistInfos) {
                for (TagInfo tag: artist.tags) {
                    if (StringUtils.startsWith(line, tag.tag + "\t")) {
                        if (VERBOSE) System.out.println(line);
                        tag.setName(line);
                    }
                }
            }
            for (OldArtistInfo artist: oldArtistInfos) {
                for (TagInfo tag: artist.tags) {
                    if (StringUtils.startsWith(line, tag.tag + "\t")) {
                        if (VERBOSE) System.out.println(line);
                        tag.setName(line);
                    }
                }
            }
        }
        tagReader.close();

        System.out.println("Finished parsing files.");
        bigPause();

        Collections.sort(recArtistInfos);
        Collections.sort(oldArtistInfos);

        System.out.println(String.format("%d recommendations found for User %s", recArtistInfos.size(), user));
        pause();

        for (RecArtistInfo recArtistInfo: recArtistInfos) {
            System.out.println(recArtistInfo.print());
            pause();
        }

        System.out.println(String.format("Recommendations are based on %d artists User %s has listened to....", oldArtistInfos.size(), user));

        System.out.println(String.format("See past artists User %s has listened to? (y/n)", user));
        char feedback = (char) System.in.read();

        if (feedback == 'y') {
            for (OldArtistInfo oldArtistInfo: oldArtistInfos) {
                System.out.println(oldArtistInfo.print());
                pause();
            }
        } else if (feedback == '5') {
            int remaining = 5;
            for (OldArtistInfo oldArtistInfo: oldArtistInfos) {
                if (remaining-- <= 0) break;
                System.out.println(oldArtistInfo.print());
                pause();
            }
        }

        System.out.printf("Finished analysis for User %s", user);
    }

    public static boolean userConfirm(String confirmMessage) throws IOException {
        System.out.println(confirmMessage + " (y/n)");
        char feedback = (char) System.in.read();
        return (feedback == 'y');
    }

    private static BufferedReader openReader(String filename) throws IOException {
        BufferedReader reader = Files.newBufferedReader(Paths.get(filename), Charset.defaultCharset());
        if (VERBOSE) System.out.println("Opened " + filename + "...");
        return reader;
    }

    private static void pause() throws IOException {
        System.out.println("Press any key to continue...");
        System.in.read();
    }

    private static void bigPause() throws IOException {
        System.out.println(new String(new char[20]).replace("\0", "-"));
        System.out.println(new String(new char[20]).replace("\0", "-"));
        System.out.println(new String(new char[20]).replace("\0", "-"));
        pause();
    }


    private static <T extends ArtistInfo> List<T> findArtists(List<T> artistInfos) throws IOException {
        String line;
        BufferedReader artistReader = openReader("data/artists.dat");
        List<T> ret = new ArrayList<>();
        if (VERBOSE) System.out.println("Finding artists: ");
        while ((line = artistReader.readLine()) != null) {
            for (ArtistInfo artistInfo: artistInfos) {
                String artistId = artistInfo.id;
                if (StringUtils.startsWith(line, artistId + "\t")) {
                    if (VERBOSE) System.out.println(line);
                    String[] ai = StringUtils.split(line, '\t');
                    artistInfo.name = ai[1];
                    artistInfo.url = ai[2];
                    artistInfo.pictureURL = ai[3];
                    ret.add((T) artistInfo);
                }
            }
        }
        artistReader.close();
        return ret;
    }

    private static <K, V> V getOrDefault(Map<K,V> map, K key, V defaultValue) {
        return map.containsKey(key) ? map.get(key) : defaultValue;
    }
}
