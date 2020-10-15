# shells-n-perls
my shell & perl scripts to keep safe and up to date.

bulk-load.py:     process and load Cisco SGSN/MME bulkstats files into Influx DB for further visualizing with Grafana  
sae-bulk-load.py: process and load Cisco GGSN/PGW bulkstats files into Influx DB for further visualizing with Grafana  

check-cfg.sh: This trivial bash script calculates MD5 checksum for running and saved configuration of asr5k and gives a warning in case of differences.

collect-ssd.sh: collects "show support details" from the list of asr5k nodes.

get-cfg.sh: collect configurations from nodes LST and save them to CVS repository

Required packages to run: bash expect lftp openssh python
