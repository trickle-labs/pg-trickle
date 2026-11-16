import json
import math
import struct
import sys

with open(sys.argv[1]) as source:
    snapshot = json.load(source)['snapshot']
old = snapshot['old_row']
old_vector = json.loads(old['v']) if old['v'] is not None else None
if old_vector is not None:
    old_vector = [struct.unpack('f', struct.pack('f', value))[0] for value in old_vector]
seed = 0x7a139465
for _ in range(1469):
    seed = (seed * 6364136223846793005 + 1) & ((1 << 64) - 1)
new_group = (seed >> 24) % 5
x = 10000.0 if seed & 1 == 0 else -10000.0
values = [x, (seed % 1000) / 17.0, 1468 / -13.0]
new_vector = []
for i in range(1536):
    offset = ((i * 17 + seed % 997) % 997) / 113.0
    value = [x + offset, -x + offset, offset / 1000000, values[1] + offset][i % 4]
    new_vector.append(struct.unpack('f', struct.pack('f', value))[0])
changes = {}
if old_vector is not None:
    changes.setdefault(old['grp'], []).append((-1, old_vector))
changes.setdefault(new_group, []).append((1, new_vector))
states = {g['grp']: g['state'] for g in snapshot['groups']}
for group, rows in changes.items():
    state = states[group].copy()
    delta = [0.0] * (2 + 2 * 1536)
    for sign, vector in rows:
        delta[0] += sign
        delta[1] += 1
        for i, value in enumerate(vector):
            delta[2 + i] += sign * value
            delta[2 + 1536 + i] += abs(value)
    state[0] += delta[0]
    state[1] += delta[1] + 2
    state[2] += delta[1]
    for i in range(2 * 1536):
        state[3 + i] += delta[2 + i]
    sums, magnitudes = state[3:1539], state[1539:]
    invalid_components = [(i, s, m) for i, (s, m) in enumerate(zip(sums, magnitudes)) if not math.isfinite(s) or not math.isfinite(m) or m < 0 or abs(s) > 2 * m]
    max_residual = max(abs(s) for s in sums)
    max_fraction = max((abs(s) / (8 * sys.float_info.epsilon * (state[1] + state[0] + 2) * m) for s, m in zip(sums, magnitudes) if m), default=0)
    print(json.dumps({'group': group, 'old_group': old['grp'], 'new_group': new_group, 'count': state[0], 'operations': state[1], 'changes': state[2], 'nonzero_sums': sum(s != 0 for s in sums), 'max_residual': max_residual, 'max_fraction_of_accumulation_bound': max_fraction, 'invalid_components': invalid_components[:5]}))
