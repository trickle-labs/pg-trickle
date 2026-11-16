import json
import os
import subprocess
import sys

sys.path.insert(0, 'benchmarks/pgvector')
import fake_embedding_worker

vector = fake_embedding_worker.embedding('seed document 1', 'model-v1', 384)
reports = []
for tenant in (1, 2):
    for iterative in ('off', 'strict_order'):
        for corpus in ('application_current', 'periodic_full_current', 'differential_current'):
            command = [sys.executable, 'benchmarks/pgvector/measure_live_rag_recall.py',
                       '--corpus', corpus, '--tenant', str(tenant), '--dimensions', '384',
                       '--query-vector', vector, '--terms', 'seed', '--k', '10',
                       '--iterative-scan', iterative, '--expected-pgvector', '0.8.7',
                       '--candidate-commit', '1e291536bf0380b37a509b730155ca3d3a0ccea4',
                       '--package-sha256', '2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b']
            result = subprocess.run(command, capture_output=True, text=True, check=True, env=os.environ)
            reports.append(json.loads(result.stdout))
            print(f'{tenant} {iterative} {corpus} complete', file=sys.stderr, flush=True)
with open('target/forced-recall-matrix.json', 'w') as destination:
    json.dump(reports, destination, indent=2, sort_keys=True)
