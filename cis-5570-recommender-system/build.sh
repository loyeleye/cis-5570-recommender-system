#!/bin/bash

case "$1" in
        clean)
            rm -rf itemProfile/ userProfile/ userProfJoin/ itemProfilePC/ userProfilePC/ cosineSimilarity/ invertedIndex/ userProfNonNorm/ userArtistCount/ output/ bloomFilter/
            mvn clean
            ;;

        build)
            mvn clean install
            ;;

        run)
            java -jar target/cis-5570-recommender-system.jar "$@:3"
            ;;

        demo)
            java -jar target/cis-5570-recommender-system-demo.jar
            ;;
        *)
            echo $"Usage: $0 { clean | build | run }"
            exit 1

esac



