#!/bin/bash -xeu

# Run after the tests are successfully completed in travis build.

if [[ "$TRAVIS_BRANCH" == "master" || "$TRAVIS_PULL_REQUEST" != "false" ]]; then
    exit 0
fi

# pip install ansible
# git clone https://github.com/CSCfi/metax-ops
# cd metax-ops/ansible/

# if [[ "$TRAVIS_BRANCH" == "test" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
#     echo "Deploying to test.."
#     ansible-galaxy -r requirements.yml install --roles-path=roles
#     ansible-playbook -vv -i inventories/test/hosts site_deploy.yml --extra-vars "ssh_user=metax-deploy-user server_domain_name=metax.fd-test.csc.fi"
# elif [[ "$TRAVIS_BRANCH" == "stable" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
#     echo "Deploying to stable.."
#     ansible-galaxy -r requirements.yml install --roles-path=roles
#     ansible-playbook -vv -i inventories/stable/hosts site_deploy.yml --extra-vars "ssh_user=metax-deploy-user server_domain_name=metax.fd-stable.csc.fi"
# fi

# # Make sure the last command to run before this part is the ansible-playbook command
# if [ $? -eq 0 ]
# then
#     exit 0
# else
#     exit 1
# fi
exit 0
