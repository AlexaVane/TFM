#!/bin/bash

KAFKA_BIN="/opt/bitnami/kafka/bin/kafka-console-producer.sh"

BOOTSTRAP="kafka-server:9092"
TOPIC="cdrs_datos"

CELDAS_FIJAS=(45678 88898 57750 78933 79824 11452 78325 88822 11114 51422 33655 58852 44445 55852 79963)
APNS=("internet.movistar.ec" "internet.tuenti.ec" "internet.claro.com" "internet.akimovil.ec")
TECNOLOGIAS=("2G" "3G" "4G" "5G")
PREFIJOS_TEL=("99" "98" "97" "96" "95" "93")


rand_num() {
  printf "%0*d" "$1" "$((RANDOM % 10**$1))"
}

rand_celda() {
  if (( RANDOM % 10 < 8 )); then
    echo "${CELDAS_FIJAS[$RANDOM % ${#CELDAS_FIJAS[@]}]}"
  else
    rand_num 5
  fi
}

rand_apn() {
  printf "%-102s" "${APNS[$RANDOM % ${#APNS[@]}]}"
}


now_epoch=$(date +%s)

echo "Inyectando CDRs sintéticos..."

while true; do

  record_type=$((18 + RANDOM % 2))
  cdrs_attr_1=$(rand_num 14)
  cdrs_attr_2=$(rand_num 14)
  id_imsi="07$(rand_num 14)"         # 16 chars
  celda=$(rand_celda)                # 5 chars
  apn=$(rand_apn)                    # 102 chars
  # imei=$(rand_num 16)                # 16 chars
  # IMEI: 16 caracteres exactos
  imei="8$(printf '%015d' $(( (RANDOM<<15 | RANDOM) % 1000000000000000 )))"

  start_time=$(date +"%Y%m%d%H%M%S")
  end_time=$(date -d "+20 minutes" +"%Y%m%d%H%M%S")
  g_end_time=$(date -d "-10 minutes" +"%Y%m%d%H%M%S")

  uplink=$(rand_num 4)
  downlink=$(rand_num 7)

  prefijo="${PREFIJOS_TEL[$RANDOM % ${#PREFIJOS_TEL[@]}]}"
  telefono="${prefijo}$(printf '%0*d' $((9 - ${#prefijo})) $((RANDOM % 10**(9 - ${#prefijo}))))"

  tecnologia="${TECNOLOGIAS[$RANDOM % ${#TECNOLOGIAS[@]}]}"

  cdr="${record_type}${cdrs_attr_1}${cdrs_attr_2}${id_imsi}${celda}${apn}${imei}${start_time}${end_time}${uplink}${downlink}${telefono}${g_end_time}${tecnologia}"

  echo "$cdr"
  sleep 5

done | $KAFKA_BIN \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC"