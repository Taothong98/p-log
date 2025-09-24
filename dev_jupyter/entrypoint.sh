#!/bin/bash

# Start SSH daemon in the background
sudo /usr/sbin/sshd

# Start Jupyter Lab
exec jupyter lab --ip=0.0.0.0 --no-browser --NotebookApp.token=${JUPYTER_TOKEN} --NotebookApp.notebook_dir=/home/master/