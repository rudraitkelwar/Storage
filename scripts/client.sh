sudo modprobe nvme
sudo modprobe nvme-tcp
sudo nvme list
#these are the IP of server, which have intiated the NVMe-TCP server via some NIC
sudo nvme discover -t tcp -a 192.168.10.16 -s 4420
sudo nvme connect  -t tcp -n nvmet-test -a 192.168.10.16 -s 4420
sudo nvme discover -t tcp -a 192.168.10.16 -s 4420
#sudo nvme connect  -t tcp -n nvmet-test5 -a 192.168.10.16 -s 4420
sudo nvme connect  -t tcp -n nvmet-test1 -a 192.168.10.16 -s 4420
