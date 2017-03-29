#!/bin/bash

echo "Deploying artifact to maven central"
read -p "Are you sure you want to deploy? (yes/NO)? " -r DO_DEPLOYMENT

if ( [ "$DO_DEPLOYMENT" == "yes" ] ); then
    read -p "Enter the version (e.g. 0.0.1) " -r VERSION
    if [ -z "$VERSION" ]; then
        echo "Version cannot be empty!"
        exit
    fi
    echo "Setting version to $VERSION"

    gradle clean build javadocJar sourcesJar uploadArchives closeRepository -Pversion="$VERSION" -Pstaging

    STATUS=$?
    if [ "$STATUS" -eq 0 ]; then
        echo "Pushing Tag to github"
        git tag "v$VERSION"
        git push --tags
    else
        echo "#################"
        echo "## BUILD ERROR ##"
        echo "#################"
    fi
else
    echo 'Canceled...'
fi
