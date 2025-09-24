sudo modprobe nvmet
sudo modprobe nvmet-tcp
sudo apt install nvme-cli
sudo nvme list

sudo /bin/mount -t configfs none /sys/kernel/config/

sudo mkdir /sys/kernel/config/nvmet/subsystems/nvmet-test2
cd /sys/kernel/config/nvmet/subsystems/nvmet-test2
echo 1 |sudo tee -a attr_allow_any_host > /dev/null
sudo mkdir namespaces/1
cd namespaces/1/
echo -n /dev/nvme0n1 |sudo tee -a device_path > /dev/null
echo 1|sudo tee -a enable > /dev/null
cd ~
sudo mkdir /sys/kernel/config/nvmet/ports/1
cd /sys/kernel/config/nvmet/ports/1 



sudo mkdir /sys/kernel/config/nvmet/subsystems/nvmet-test3
cd /sys/kernel/config/nvmet/subsystems/nvmet-test3
echo 1 |sudo tee -a attr_allow_any_host > /dev/null
sudo mkdir namespaces/2
cd namespaces/2/
echo -n /dev/nvme1n1 |sudo tee -a device_path > /dev/null
echo 1|sudo tee -a enable > /dev/null
cd ~
sudo mkdir /sys/kernel/config/nvmet/ports/1
cd /sys/kernel/config/nvmet/ports/1


echo 192.168.10.18 |sudo tee -a addr_traddr > /dev/null
echo tcp|sudo tee -a addr_trtype > /dev/null
echo 4420|sudo tee -a addr_trsvcid > /dev/null
echo ipv4|sudo tee -a addr_adrfam > /dev/null
cd ~
sudo ln -s /sys/kernel/config/nvmet/subsystems/nvmet-test2/ /sys/kernel/config/nvmet/ports/1/subsystems/nvmet-test2
sudo ln -s /sys/kernel/config/nvmet/subsystems/nvmet-test3/ /sys/kernel/config/nvmet/ports/1/subsystems/nvmet-test3
dmesg |grep "nvmet_tcp"
sudo systemctl stop firewalld
