Vagrant.configure('2') do |config|

  # Every Vagrant virtual environment requires a box to build off of.
  config.vm.box = 'ubuntu/trusty64'
  
  config.ssh.forward_agent = true

  config.vm.network "private_network", ip: "192.168.50.10"
  config.vm.provider :virtualbox do |vb|
    vb.customize ['modifyvm', :id, '--memory', 2048]
    vb.customize ['modifyvm', :id, '--ioapic', 'on']
  end

  config.vm.provision :shell do |external_shell|
    external_shell.path = 'vagrant_files/setup.sh'
  end
end
