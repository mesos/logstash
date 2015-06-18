#!/bin/bash -e

apt-get -y update
apt-get -y install linux-image-generic-lts-trusty
sudo apt-get install wget

# Docker setup
echo "Installing docker"
wget -qO- https://get.docker.com/ | sh

echo "Updating docker config to ignore SELinux and to accept http"
cp -f /vagrant/vagrant_files/etc/sysconfig/docker.new /etc/default/docker

echo "Adding env vars in profile.d"
cp -f /vagrant/vagrant_files/etc/profile.d/env_vars.sh /etc/profile.d/

echo "Restarting docker"
service docker restart

echo "Installing docker-compose"
curl -L https://github.com/docker/compose/releases/download/1.2.0/docker-compose-`uname -s`-`uname -m` > /usr/bin/docker-compose
chmod +x /usr/bin/docker-compose

echo "Installing jdk"
apt-get -y install default-jdk

echo "Installing mesos"

# According to the guide from mesosphere https://docs.mesosphere.com/tutorials/install_ubuntu_debian/#overview
apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)
echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | tee /etc/apt/sources.list.d/mesosphere.list
apt-get -y update
apt-get -y install mesos marathon

echo "Installing zookeeper"
apt-get install -y zookeeperd python-setuptools
echo 1 | tee -a /var/lib/zookeeper/myid >/dev/null

echo "Done!"
