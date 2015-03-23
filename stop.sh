#!/bin/sh

# Outputs the whole log of the last app.
last=`yarn application -list 2>/dev/null | grep -o "^application_[0-9]*_[0-9]*"`
yarn application -kill "$last"
