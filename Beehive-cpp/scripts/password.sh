#!/bin/bash
PASSWD="your password"

if [ -z $PASSWD ]; then
    read -p "Enter your password: "$'\n' -rs PASSWD
    echo $PASSWD | sudo -S -p "" echo
fi
