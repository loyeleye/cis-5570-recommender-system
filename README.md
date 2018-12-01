# cis-5570-recommender-system
Recommender System using last.fm dataset

## Installing IntelliJ
1. Go to https://www.jetbrains.com/idea/download/#section=windows
2. Select your Operating System.
3. Download the "Community Edition" and click to install when complete.
4. Follow the installation steps.

## Running project as a Maven Jar file
(Have to pull the latest changes from master)
Run the following command in the same directory as the pom.xml:<br/>
`mvn clean compile assembly:single`<br/>
Then run the new jar file:<br/>
`java -jar target/cis-5570...(just press <tab>).jar`<br/>
[Click here](https://stackoverflow.com/questions/34070599/intellij-maven-create-jar-with-all-library-dependencies) for more details.


# Added build.sh in "cis-5570-recommender-system"
  ./build.sh clean (removes the itemProfile &  userProfile for running)
             build
             run



# some basic git commands
# clone this project
  git clone git@github.com:loyeleye/cis-5570-recommender-system.git  or https://github.com/loyeleye/cis-5570-recommender-system.git
# add your changes
  git add --all  (this will also add directories and their files)
# commit
  git commit -m "comments"
# push
  git push -u origin master
# getting the latest changes from remote
  git pull
# reset your local project to last commit # warning this will destroy your local changes if they haven't been committed & push'd
  git reset --hard origin/master
# save your local changes (just in case)
  git stash save

