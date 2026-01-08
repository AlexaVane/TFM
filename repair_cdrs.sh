#!/bin/bash

LOG=/var/log/repair_cdrs.log

 /opt/hive/bin/beeline -u "jdbc:hive2://127.0.0.1:10000" -n hive -e "MSCK REPAIR TABLE db_trafico.cdrs_datos;" >> "$LOG" 2>&1
