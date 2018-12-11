#!/bin/bash

case "$1" in
        clean)
            rm -rf itemProfile/ userProfile/ userProfJoin/ itemProfilePC/ userProfilePC/ cosineSimilarity/ invertedIndex/ userProfNonNorm/ userArtistCount/
            mvn clean
            ;;

        build)
            mvn clean compile assembly:single
            ;;

        run)
            java -jar target/cis-5570-recommender-system-1.0-SNAPSHOT-jar-with-dependencies.jar
            ;;
        *)
            echo $"Usage: $0 { clean | build | run }"
            exit 1

esac



