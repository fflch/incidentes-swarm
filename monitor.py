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
    """Captura falhas de Replicated Jobs (ex: php artisan migrate, drush updb/initupdb)."""
    conn = None
    cursor = None
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
                        type, id_externo, service_name, data_ocorrencia, mensagem, status_resolucao, created_at, updated_at
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

    except Exception as e:
        print(f"Erro ao buscar tarefas com falhas no Swarm: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def fetch_container_log_errors(last_check_timestamp):
    """Captura exceções 500 emitidas nos logs (stderr/stdout) de aplicações Laravel."""
    conn = None
    cursor = None
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
                error_fingerprint = f"{service_name}:{first_line}:{data_ocorrencia.strftime('%Y-%m-%d-%H')}"
                id_externo = hashlib.md5(error_fingerprint.encode('utf-8')).hexdigest()

                full_service_name = f"{stack_name}/{service_name}" if stack_name else service_name
                full_service_name = full_service_name[:100]

                mensagem_completa = f"Erro de App (Laravel 500):\n{entry.strip()}"

                insert_query = """
                    INSERT IGNORE INTO alerts (
                        type, id_externo, service_name, data_ocorrencia, mensagem, status_resolucao, created_at, updated_at
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

    except Exception as e:
        print(f"Erro ao buscar logs de erros Laravel: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


def fetch_drupal_stack_errors():
    """
    Captura erros de Watchdog (4 severidades) e do Relatório de Status (drush rq --severity=2)
    em cada stack Drupal do Swarm (executando em APENAS UMA réplica por site).
    """
    conn = None
    cursor = None
    try:
        client = docker.from_env()
        conn = get_db_connection()
        cursor = conn.cursor()

        containers = client.containers.list(filters={"status": "running"})
        
        # Agrupa containers por stack do Swarm para rodar Drush em APENAS UMA réplica por site
        drupal_stacks = {}

        for container in containers:
            image_name = container.image.tags[0] if container.image.tags else ""
            container_name = container.name

            # Identifica se é um container Drupal principal
            if "drupal" not in image_name.lower() and "drupal" not in container_name.lower():
                continue

            # Ignora os containers de cron ou de migração initupdb
            if "cron" in container_name.lower() or "initupdb" in container_name.lower():
                continue

            labels = container.labels
            stack_name = labels.get("com.docker.stack.namespace", container_name)

            # Guarda apenas o primeiro container encontrado por stack (evita duplicar drush nas réplicas)
            if stack_name not in drupal_stacks:
                drupal_stacks[stack_name] = container

        severidades_criticas = ["1", "2", "3"]

        for stack_name, container in drupal_stacks.items():
            labels = container.labels
            service_name = labels.get("com.docker.swarm.service.name", container.name)
            full_service_name = f"{stack_name}/{service_name}" if stack_name else service_name
            full_service_name = full_service_name[:100]

            # ----------------------------------------------------
            # 1. MONITORAMENTO DO WATCHDOG (4 Severidades)
            # ----------------------------------------------------
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
                    items = logs.values() if isinstance(logs, dict) else logs

                    for item in items:
                        wid = item.get("wid") or item.get("id")
                        if not wid:
                            continue

                        # ID Único absoluto via WID do banco do Drupal
                        id_externo = f"drupal-watchdog-{stack_name}-{wid}"

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
                                type, id_externo, service_name, data_ocorrencia, mensagem, status_resolucao, created_at, updated_at
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
                except Exception as e:
                    print(f"Erro ao consultar watchdog [{severidade}] na stack {stack_name}: {e}")

            # ----------------------------------------------------
            # 2. MONITORAMENTO DO RELATÓRIO DE STATUS (drush rq)
            # ----------------------------------------------------
            try:
                exec_rq = container.exec_run(
                    "vendor/bin/drush rq --severity=2 --format=json",
                    workdir="/var/www/html"
                )

                if exec_rq.exit_code == 0:
                    output_rq = exec_rq.output.decode('utf-8', errors='ignore').strip()
                    if output_rq and output_rq != "[]" and output_rq != "{}":
                        rq_items = json.loads(output_rq)
                        items_dict = rq_items if isinstance(rq_items, dict) else {}

                        for req_key, req_data in items_dict.items():
                            title = req_data.get("title", req_key)
                            value = req_data.get("value", "")
                            description = req_data.get("description", "")

                            # ID Único diário por item de erro do Status Report por site
                            id_externo = f"drupal-rq-{stack_name}-{req_key}-{datetime.utcnow().strftime('%Y-%m-%d')}"

                            mensagem_completa = (
                                f"Erro no Relatório de Status do Drupal (Status Report)\n"
                                f"Item: {title}\n"
                                f"Valor: {value}\n"
                                f"Descrição: {description}\n"
                                f"Comando para investigar: vendor/bin/drush status-report"
                            )

                            insert_query = """
                                INSERT IGNORE INTO alerts (
                                    type, id_externo, service_name, data_ocorrencia, mensagem, status_resolucao, created_at, updated_at
                                ) VALUES (%s, %s, %s, NOW(), %s, 'pendente', NOW(), NOW())
                            """

                            cursor.execute(insert_query, (
                                'drupal',
                                id_externo,
                                full_service_name,
                                mensagem_completa
                            ))
                            conn.commit()
            except Exception as e:
                print(f"Erro ao consultar status report na stack {stack_name}: {e}")

    except Exception as e:
        print(f"Erro na função geral de verificação do Drupal: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


if __name__ == "__main__":
    print("Iniciando monitoramento unificado (Jobs Swarm, Laravel 500, Drupal Watchdog e Status Report)...")
    last_check_timestamp = time.time() - POLL_INTERVAL

    while True:
        current_time = time.time()
        try:
            # 1. Checa falhas em Jobs Swarm (artisan migrate, drush updb/initupdb, etc.)
            fetch_failed_tasks()

            # 2. Checa erros 500 em containers Laravel em execução (stderr)
            fetch_container_log_errors(last_check_timestamp)

            # 3. Checa erros de Watchdog e Status Report em cada site Drupal no Swarm
            fetch_drupal_stack_errors()

            last_check_timestamp = current_time
        except Exception as e:
            print(f"Erro durante o loop principal de monitoramento: {e}")
            
        time.sleep(POLL_INTERVAL)
