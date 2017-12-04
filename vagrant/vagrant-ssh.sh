#!/bin/bash

set -x

# Create hashdata user
#sudo groupadd -g 555 hashdata
#sudo useradd -u 555 -g 555 hashdata
sudo groupadd -g 555 hashdata
sudo useradd -u 555 -g 555 hashdata
sudo echo "%hashdata ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/hashdata

SSH_DIR=/home/hashdata/.ssh
AUTH_FILE="$SSH_DIR"/authorized_keys
PRIVATE_KEY_FILE="$SSH_DIR"/id_rsa

sudo rm -rf "$SSH_DIR"

sudo mkdir "$SSH_DIR"
sudo chmod 0700 "$SSH_DIR"
sudo cat /vagrant/id_rsa.pub > "$AUTH_FILE"
sudo cat /vagrant/id_rsa > "$PRIVATE_KEY_FILE"
sudo chmod 0600 "$AUTH_FILE"
sudo chmod 0600 "$PRIVATE_KEY_FILE"
sudo chown -R hashdata "$SSH_DIR"
sudo chgrp -R hashdata "$SSH_DIR"

sudo echo "export JAVA_HOME=/etc/alternatives/jre" >> /home/hashdata/.bash_profile
