#!/usr/bin/env bash


data=$1

awk -F';' '
{ 
  station = $1
  value   = $2 + 0
 
  if (station in cnt) {
    if (value < min[station]) min[station] = value
    if (value > max[station]) max[station] = value
    sum[station] += value
    cnt[station] += 1
  } else {
    min[station] = value
    max[station] = value
    sum[station] = value
    cnt[station] = 1
  }
}
END {
  for (s in cnt) {
    avg = sum[s] / cnt[s]
    print s, min[s], max[s], avg
  }
}
' "$data" | sort -k1
