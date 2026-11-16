import json
import sys

source, output = sys.argv[1:]
records = {}
with open(source, encoding='utf-8') as log:
    collecting = False
    lines = []
    for line in log:
        if ' LOG:  duration:' in line and ' plan:' in line:
            collecting = True
            lines = []
            continue
        if not collecting:
            continue
        pieces = line.split(' ', 1)
        if len(pieces) != 2:
            continue
        lines.append(pieces[1])
        try:
            record = json.loads(''.join(lines))
        except json.JSONDecodeError:
            continue
        collecting = False
        query = record.get('Query Text', '')
        if '__pgt_distance_changed_before_10_0' in query:
            if query.startswith('WITH before_rows'):
                records.setdefault('distance', record)
            elif query.startswith('WITH b AS MATERIALIZED'):
                records.setdefault('compaction', record)
with open(output, 'w', encoding='utf-8') as result:
    json.dump(records, result, indent=2)
    result.write('\n')
print(source, sorted(records))
