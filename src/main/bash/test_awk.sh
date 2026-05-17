#!/usr/bin/env bash

data=$1

export LC_ALL=C

partial_awk=$(mktemp)
merge_awk=$(mktemp)
trap 'rm -f "$partial_awk" "$merge_awk"' EXIT

cat > "$partial_awk" <<'EOF'
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
    print s ";" min[s] ";" max[s] ";" sum[s] ";" cnt[s]
  }
}
EOF

cat > "$merge_awk" <<'EOF'
{
  station = $1
  mn = $2 + 0
  mx = $3 + 0
  sm = $4 + 0
  ct = $5 + 0
  if (station in cnt) {
    if (mn < min[station]) min[station] = mn
    if (mx > max[station]) max[station] = mx
    sum[station] += sm
    cnt[station] += ct
  } else {
    min[station] = mn
    max[station] = mx
    sum[station] = sm
    cnt[station] = ct
  }
}
END {
  for (s in cnt) {
    avg = sum[s] / cnt[s]
    print s, min[s], max[s], avg
  }
}
EOF

parallel -q --pipepart -a "$data" --block 100M -j "$(nproc)" \
    mawk -F\; -f "$partial_awk" \
  | mawk -F\; -f "$merge_awk" \
  | sort -k1
