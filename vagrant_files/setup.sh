#!/bin/bash -e

echo "Setting up useful docker bash utils"
cp -f /vagrant/vagrant_files/bashutils /home/vagrant/.bashutils
echo "source ~/.bashutils" >> /home/vagrant/.bashrc
chown vagrant -R /home/vagrant


apt-get -y update
apt-get -y install linux-image-generic-lts-trusty
sudo apt-get install wget

echo "Installing protobuf-compiler"
apt-get -y install protobuf-compiler

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
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
add-apt-repository -y ppa:webupd8team/java
apt-get update
apt-get install -y oracle-java8-installer

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
