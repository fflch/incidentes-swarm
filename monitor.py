import os
import time
import re
import json
import hashlib
from datetime import datetime
import dateutil.parser
import docker
import mysql.connector

# Configurações do Banco via Variáveis de Ambiente
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "database")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def fetch_failed_tasks():
    """Captura falhas de Replicated Jobs (ex: php artisan migrate, drush updb)."""
    try:
        client = docker.from_env()
        tasks = client.api.tasks(filters={"desired-state": "shutdown"})
        
        conn = get_db_connection()
        cursor = conn.cursor()

        for task in tasks:
            task_status = task.get("Status", {})
            state = task_status.get("State")
            
            if state in ["failed", "rejected"]:
                service_id = task.get("ServiceID")
                
                try:
                    service = client.services.get(service_id)
                    service_mode = service.attrs.get("Spec", {}).get("Mode", {})
                    
                    if "ReplicatedJob" not in service_mode:
                        continue

                    service_name = service.name
                    labels = service.attrs.get("Spec", {}).get("Labels", {})
                    stack_name = labels.get("com.docker.stack.namespace", "")
                except Exception:
                    continue

                task_id = task.get("ID")
                timestamp_str = task_status.get("Timestamp")
                data_ocorrencia = dateutil.parser.parse(timestamp_str) if timestamp_str else datetime.utcnow()
                
                error_msg = task_status.get("Err", "Erro desconhecido informado pelo Swarm")

                full_service_name = f"{stack_name}/{service_name}" if stack_name else service_name
                full_service_name = full_service_name[:100]

                container_status = task_status.get("ContainerStatus", {})
                container_id = container_status.get("ContainerID")
                logs = ""
                if container_id:
                    try:
                        container = client.containers.get(container_id)
                        logs = container.logs(tail=20).decode('utf-8', errors='ignore')
                    except Exception:
                        logs = "(Não foi possível capturar os logs do container)"

                mensagem_completa = f"Erro Swarm (Job): {error_msg}\n\n--- Últimos Logs ---\n{logs}"

                insert_query = """
                    INSERT IGNORE INTO alerts (
                        type, 
                        id_externo, 
                        service_name, 
                        data_ocorrencia, 
                        mensagem, 
                        status_resolucao, 
                        created_at, 
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, 'pendente', NOW(), NOW())
                """
                
                cursor.execute(insert_query, (
                    'docker',
                    task_id,
                    full_service_name,
                    data_ocorrencia,
                    mensagem_completa
                ))
                conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao buscar tarefas com falhas no Swarm: {e}")


def fetch_container_log_errors(last_check_timestamp):
    """Captura exceções 500 emitidas nos logs (stderr/stdout) de aplicações Laravel."""
    try:
        client = docker.from_env()
        conn = get_db_connection()
        cursor = conn.cursor()

        containers = client.containers.list(filters={"status": "running"})

        for container in containers:
            labels = container.labels
            service_name = labels.get("com.docker.swarm.service.name", container.name)
            stack_name = labels.get("com.docker.stack.namespace", "")

            try:
                raw_logs = container.logs(since=int(last_check_timestamp), stderr=True, stdout=True).decode('utf-8', errors='ignore')
            except Exception:
                continue

            if not raw_logs.strip():
                continue

            # Separador das entradas de log do Laravel usando regex com o timestamp [YYYY-MM-DD HH:MM:SS]
            log_entries = re.split(r'(?=\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\])', raw_logs)

            for entry in log_entries:
                if not entry.strip() or not re.search(r'\.(ERROR|CRITICAL|EMERGENCY|ALERT):', entry):
                    continue

                date_match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', entry)
                if date_match:
                    try:
                        data_ocorrencia = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        data_ocorrencia = datetime.utcnow()
                else:
                    data_ocorrencia = datetime.utcnow()

                first_line = entry.strip().split('\n')[0]
                # Agrupa hash MD5 por erro + serviço + hora para evitar duplicação em loop
                error_fingerprint = f"{service_name}:{first_line}:{data_ocorrencia.strftime('%Y-%m-%d-%H')}"
                id_externo = hashlib.md5(error_fingerprint.encode('utf-8')).hexdigest()

                full_service_name = f"{stack_name}/{service_name}" if stack_name else service_name
                full_service_name = full_service_name[:100]

                mensagem_completa = f"Erro de App (Laravel 500):\n{entry.strip()}"

                insert_query = """
                    INSERT IGNORE INTO alerts (
                        type, 
                        id_externo, 
                        service_name, 
                        data_ocorrencia, 
                        mensagem, 
                        status_resolucao, 
                        created_at, 
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, 'pendente', NOW(), NOW())
                """

                cursor.execute(insert_query, (
                    'docker',
                    id_externo,
                    full_service_name,
                    data_ocorrencia,
                    mensagem_completa
                ))
                conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao buscar logs de erros Laravel: {e}")


def fetch_drupal_watchdog_errors():
    """Captura erros graves (Emergência, Alerta, Crítico, Erro) do Watchdog do Drupal via Drush."""
    try:
        client = docker.from_env()
        conn = get_db_connection()
        cursor = conn.cursor()

        containers = client.containers.list(filters={"status": "running"})
        severidades_criticas = ["Emergência", "Alerta", "Crítico", "Erro"]

        for container in containers:
            image_name = container.image.tags[0] if container.image.tags else ""
            container_name = container.name

            # Filtra apenas containers Drupal
            if "drupal" not in image_name.lower() and "drupal" not in container_name.lower():
                continue

            # Ignora containers de cron ou initupdb
            if "cron" in container_name.lower() or "initupdb" in container_name.lower():
                continue

            labels = container.labels
            service_name = labels.get("com.docker.swarm.service.name", container.name)
            stack_name = labels.get("com.docker.stack.namespace", "")
            full_service_name = f"{stack_name}/{service_name}" if stack_name else service_name
            full_service_name = full_service_name[:100]

            for severidade in severidades_criticas:
                try:
                    exec_res = container.exec_run(
                        f'vendor/bin/drush watchdog:show --severity="{severidade}" --format=json --count=20',
                        workdir="/var/www/html"
                    )

                    if exec_res.exit_code != 0:
                        continue

                    output = exec_res.output.decode('utf-8', errors='ignore').strip()
                    if not output or output == "[]":
                        continue

                    logs = json.loads(output)
                except Exception:
                    continue

                items = logs.values() if isinstance(logs, dict) else logs

                for item in items:
                    wid = item.get("wid") or item.get("id")
                    if not wid:
                        continue

                    # Identificador único usando o WID nativo do Drupal por serviço
                    id_externo = f"drupal-{service_name}-{wid}"

                    type_str = item.get("type", "drupal")
                    severity_str = item.get("severity", severidade)
                    message_raw = item.get("message", "")
                    date_str = item.get("date", "")
                    location = item.get("location", "N/A")

                    try:
                        data_ocorrencia = dateutil.parser.parse(date_str) if date_str else datetime.utcnow()
                    except Exception:
                        data_ocorrencia = datetime.utcnow()

                    mensagem_completa = (
                        f"Erro Drupal Watchdog [ID: {wid}] [{severity_str}]\n"
                        f"Tipo: {type_str}\n"
                        f"URL/Location: {location}\n"
                        f"Comando para investigar: vendor/bin/drush watchdog:show {wid}\n\n"
                        f"Detalhes: {message_raw}"
                    )

                    insert_query = """
                        INSERT IGNORE INTO alerts (
                            type, 
                            id_externo, 
                            service_name, 
                            data_ocorrencia, 
                            mensagem, 
                            status_resolucao, 
                            created_at, 
                            updated_at
                        ) VALUES (%s, %s, %s, %s, %s, 'pendente', NOW(), NOW())
                    """

                    cursor.execute(insert_query, (
                        'drupal',
                        id_externo,
                        full_service_name,
                        data_ocorrencia,
                        mensagem_completa
                    ))
                    conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao consultar watchdog do Drupal: {e}")


if __name__ == "__main__":
    print("Iniciando monitoramento unificado (Jobs Swarm, Laravel 500 e Drupal Watchdog)...")
    last_check_timestamp = time.time() - POLL_INTERVAL

    while True:
        current_time = time.time()
        try:
            # 1. Checa falhas em Jobs Swarm (artisan migrate, drush updb, etc.)
            fetch_failed_tasks()

            # 2. Checa erros 500 em containers Laravel em execução (stderr)
            fetch_container_log_errors(last_check_timestamp)

            # 3. Checa erros de severidade crítica (0 a 3) no Watchdog do Drupal via Drush
            fetch_drupal_watchdog_errors()

            last_check_timestamp = current_time
        except Exception as e:
            print(f"Erro durante o loop principal de monitoramento: {e}")
            
        time.sleep(POLL_INTERVAL)
