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
        for case in ('10000_0_0', '10000_1000_0', '10000_0_1', '10000_1000_1'):
            if f'alert_measure_st_{case}' in query and ('COUNT(*)' in query.upper() or '<->' in query):
                key = f"{case}:{'distance' if '<->' in query else 'count'}"
                records.setdefault(key, record)
with open(output, 'w', encoding='utf-8') as result:
    json.dump(records, result, indent=2)
    result.write('\n')
print(source, len(records), sorted(records))
