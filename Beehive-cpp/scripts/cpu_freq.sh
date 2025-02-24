#!/bin/bash

# Configure cpu frequency for all cores
# https://www.kernel.org/doc/Documentation/cpu-freq/user-guide.txt
#
# @param1: enable | disable

if [[ ! -z $1 && $1 != "enable" && $1 != "disable" ]]; then
  echo "cpu-freq.sh invalid argument: $1" >&2
  echo ""
  echo "Usage: $(basename $0) [disable|enable]"
  exit 1
fi

if [[ -e /sys/devices/system/cpu/intel_pstate ]]; then
  echo "Error: userspace governor is unavailable with intel_pstate,"
  echo "please disable it in kernel cmdline with 'intel_pstate=disable'."
  exit 1
fi

cores=$(cat /proc/cpuinfo | grep processor | awk '{print $3}')

# Fix cpu frequency for experiment
if [[ $1 == "enable" ]]; then
  for i in $cores;
  do
    # 1. Enable userspace governor for acpi-cpufreq driver
    #    there is only performance and powersave govenors for intel_pstate driver
    #    disable it in kernel cmdline with "intel_pstate=disable".
    sudo sh -c "echo userspace > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_governor"
    # 2. Must set both of the max and min freq
    sudo sh -c "echo 2401000 > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_max_freq"
    sudo sh -c "echo 2400000 > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_min_freq"
  done
fi

# Recover to daily usage situation
if [[ $1 == "disable" ]]; then
  for i in $cores;
  do
    # 1. Enable userspace governor
    sudo sh -c "echo userspace > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_governor"
    # 2. Must set both of the max and min freq
    sudo sh -c "echo 2401000 > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_max_freq"
    sudo sh -c "echo 1000000 > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_min_freq"
    # 3. Enable ondemand governor
    sudo sh -c "echo ondemand > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_governor"
  done
fi

state=$(cat /sys/devices/system/cpu/cpu5/cpufreq/scaling_governor)
echo "CPU5 freq governor: $state"
state=$(cat /sys/devices/system/cpu/cpu5/cpufreq/scaling_max_freq)
echo "CPU5 freq max: $state"
state=$(cat /sys/devices/system/cpu/cpu5/cpufreq/scaling_min_freq)
echo "CPU5 freq min: $state"
state=$(cat /sys/devices/system/cpu/cpu5/cpufreq/scaling_cur_freq)
echo "CPU5 freq cur: $state"

