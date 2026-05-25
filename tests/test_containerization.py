import subprocess
import time


def run_command(cmd):
    print('Running:', ' '.join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise SystemExit(result.returncode)


if __name__ == '__main__':
    run_command(['docker', 'compose', 'up', '--build', '-d'])
    time.sleep(15)

    run_command(['docker', 'compose', 'ps'])

    print('Vérification du topic Kafka (nécessite kafka-console-consumer):')
    # Ce test peut être amélioré selon l'environnement.
    run_command(['docker', 'compose', 'exec', '-T', 'redpanda', 'rpk', 'topic', 'list'])

    print('Vérification des fichiers de sortie dans le conteneur pyspark:')
    run_command(['docker', 'compose', 'exec', '-T', 'pyspark', 'bash', '-c', 'ls -R /app/output || true'])

    print('Arrêt')
    run_command(['docker', 'compose', 'down', '-v'])
    print('Succès: conteneurisation testée.')
