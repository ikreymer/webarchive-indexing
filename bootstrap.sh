#!/bin/bash

# bootstrap commands
sudo yum install -y python27 python27-devel gcc-c++ git libffi-devel
sudo curl -o /tmp/get-pip.py https://bootstrap.pypa.io/get-pip.py
sudo python2.7 /tmp/get-pip.py
# http://superuser.com/questions/762185/python2-7-pip2-7-install-in-centos6-root-does-not-see-usr-local-bin
sudo /usr/local/bin/pip2.7 install boto mrjob simplejson
sudo /usr/local/bin/pip2.7 install pywb
#sudo pip2.7 install -e "git+git://github.com/ikreymer/pywb.git@develop#egg=pywb-0.9.7-dev"
