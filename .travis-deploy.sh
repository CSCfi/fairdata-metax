#!/bin/bash -xeu

# Run after the tests are successfully completed in travis build.

if [[ "$TRAVIS_BRANCH" == "playground" || "$TRAVIS_BRANCH" == "master" || "$TRAVIS_PULL_REQUEST" != "false" ]]; then
    exit 0
fi

git clone https://github.com/CSCfi/metax-ops
cd metax-ops/ansible/

if [[ "$TRAVIS_BRANCH" == "test" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
    echo "Deploying to test.."
    ansible-galaxy -r requirements.yml install --roles-path=roles
    ansible-playbook -vv -i inventories/test/hosts site_deploy.yml --extra-vars "ssh_user=metax-user"
elif [[ "$TRAVIS_BRANCH" == "stable" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
    echo "Deploying to stable.. (NOT ACTUALLY, ADD CODE TO DO IT)"
fi

# Make sure the last command to run before this part is the ansible-playbook command
if [ $? -eq 0 ]
then
    exit 0
else
    exit 1
fi