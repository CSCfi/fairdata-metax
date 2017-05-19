#!/bin/bash -xeu

# Run as the after_success step of a travis build.

if [[ "$TRAVIS_BRANCH" == "playground" ]]; then
    exit 0
fi

git clone https://github.com/CSCfi/metax-ops
cd metax-ops/ansible/

if [[ "$TRAVIS_BRANCH" == "staging" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
    echo "Staging"
    ansible-playbook -vv -i environments/staging site_deploy.yml
elif [[ "$TRAVIS_BRANCH" == "stable" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
    echo "Stable"
elif [[ "$TRAVIS_BRANCH" == "master" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
    echo "Master"
fi