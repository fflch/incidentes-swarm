import os
import time
import re
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
    """Captura falhas de Replicated Jobs (ex: php artisan migrate)."""
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


def fetch_container_log_errors(last_check_timestamp):
    """Captura exceções 500 emitidas nos logs (stderr) dos containers em execução."""
    client = docker.from_env()
    conn = get_db_connection()
    cursor = conn.cursor()

    # Busca containers em execução no nó/swarm
    containers = client.containers.list(filters={"status": "running"})

    for container in containers:
        labels = container.labels
        service_name = labels.get("com.docker.swarm.service.name", container.name)
        stack_name = labels.get("com.docker.stack.namespace", "")

        try:
            # Captura logs desde a última checagem
            raw_logs = container.logs(since=int(last_check_timestamp), stderr=True, stdout=True).decode('utf-8', errors='ignore')
        except Exception:
            continue

        if not raw_logs.strip():
            continue

        # Quebra as entradas de log do Laravel usando a regex do padrão de data [YYYY-MM-DD HH:MM:SS]
        log_entries = re.split(r'(?=\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\])', raw_logs)

        for entry in log_entries:
            # Filtra apenas linhas que contêm níveis de erro no log do Laravel
            if not entry.strip() or not re.search(r'\.(ERROR|CRITICAL|EMERGENCY|ALERT):', entry):
                continue

            # Extrai a data do log [YYYY-MM-DD HH:MM:SS]
            date_match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', entry)
            if date_match:
                try:
                    data_ocorrencia = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    data_ocorrencia = datetime.utcnow()
            else:
                data_ocorrencia = datetime.utcnow()

            # Pega a primeira linha da exceção para gerar um hash MD5 único por hora
            first_line = entry.strip().split('\n')[0]
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


if __name__ == "__main__":
    print("Iniciando monitoramento de falhas e logs no Docker Swarm...")
    last_check_timestamp = time.time() - POLL_INTERVAL

    while True:
        current_time = time.time()
        try:
            # 1. Verifica falhas em Jobs (artisan migrate)
            fetch_failed_tasks()

            # 2. Verifica erros 500 nos logs dos containers em execução
            fetch_container_log_errors(last_check_timestamp)

            last_check_timestamp = current_time
        except Exception as e:
            print(f"Erro durante a execução do monitor: {e}")
            
        time.sleep(POLL_INTERVAL)
