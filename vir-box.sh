#!/bin/bash

sudo modprobe -r kvm_intel
sudo modprobe -r kvm_amd
sudo modprobe -r kvm
