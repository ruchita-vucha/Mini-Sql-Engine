#!/bin/sh
read -p "Query please " -r query
python3 mainsql.py "$query"
