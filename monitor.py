import os
import time
import re
import json
import hashlib
from datetime import datetime, timedelta
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

def adjust_datetime(dt=None):
    """Retorna o datetime (ou o atual) ajustado com a subtração de 3 horas (sem timezone)."""
    if dt is None:
        dt = datetime.utcnow()
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt - timedelta(hours=3)


HOST_RULE_RE = re.compile(r"Host\(\s*`([^`]+)`\s*\)")


def _pick_host(hosts):
    """Escolhe o host principal: prefere o que não começa com 'www.' e, entre eles, o mais curto."""
    hosts = [h.strip().lower() for h in hosts if h and h.strip()]
    if not hosts:
        return None
    sem_www = [h for h in hosts if not h.startswith("www.")]
    candidatos = sem_www or hosts
    return sorted(candidatos, key=len)[0]


def build_stack_url_map(client):
    """
    Monta um dicionário {stack_name: host} a partir dos serviços do Swarm.
    1º tenta as regras do Traefik (traefik.http.routers.*.rule=Host(`...`)) nas labels do serviço;
    2º usa as variáveis de ambiente SITE_HOST (Drupal) ou APP_URL (Laravel).
    Todos os serviços da stack (worker, cron, migrate, initupdb...) herdam o host encontrado.
    """
    traefik_hosts = {}
    env_hosts = {}
    try:
        services = client.services.list()
    except Exception as e:
        print(f"Erro ao listar serviços para resolver URLs: {e}")
        return {}

    for service in services:
        spec = service.attrs.get("Spec", {})
        labels = spec.get("Labels", {}) or {}
        stack_name = labels.get("com.docker.stack.namespace")
        if not stack_name:
            continue

        for key, value in labels.items():
            if key.startswith("traefik.http.routers.") and key.endswith(".rule"):
                traefik_hosts.setdefault(stack_name, []).extend(HOST_RULE_RE.findall(value or ""))

        env_list = spec.get("TaskTemplate", {}).get("ContainerSpec", {}).get("Env", []) or []
        for env in env_list:
            name, _, val = env.partition("=")
            if name == "SITE_HOST" and val:
                env_hosts.setdefault(stack_name, []).append(val)
            elif name == "APP_URL" and val:
                host = re.sub(r"^https?://", "", val).split("/")[0]
                env_hosts.setdefault(stack_name, []).append(host)

    url_map = {}
    for stack_name in set(traefik_hosts) | set(env_hosts):
        host = _pick_host(traefik_hosts.get(stack_name, [])) or _pick_host(env_hosts.get(stack_name, []))
        if host:
            url_map[stack_name] = host
    return url_map


def _detect_service_type(spec):
    """Identifica o framework de um serviço pelo spec: 'drupal', 'laravel' ou None."""
    container_spec = spec.get("TaskTemplate", {}).get("ContainerSpec", {})
    image = (container_spec.get("Image") or "").lower()
    if "drupal" in image:
        return "drupal"

    env_names = {e.partition("=")[0] for e in (container_spec.get("Env") or [])}
    command = " ".join((container_spec.get("Command") or []) + (container_spec.get("Args") or []))
    if "APP_KEY" in env_names or "artisan" in command:
        return "laravel"
    if "SITE_HOST" in env_names or "drush" in command:
        return "drupal"
    return None


def build_stack_type_map(client):
    """Monta {stack_name: 'drupal' | 'laravel'} a partir dos serviços de cada stack."""
    type_map = {}
    try:
        services = client.services.list()
    except Exception as e:
        print(f"Erro ao listar serviços para resolver tipos: {e}")
        return {}

    for service in services:
        spec = service.attrs.get("Spec", {})
        stack_name = (spec.get("Labels", {}) or {}).get("com.docker.stack.namespace")
        if not stack_name:
            continue
        service_type = _detect_service_type(spec)
        # 'drupal' tem prioridade, pois é a detecção mais específica (pela imagem)
        if service_type and type_map.get(stack_name) != "drupal":
            type_map[stack_name] = service_type
    return type_map


def resolve_type(type_map, stack_name, default="docker"):
    """Retorna o tipo do alerta para a stack ('drupal', 'laravel' ou o padrão)."""
    return type_map.get(stack_name, default) if stack_name else default


def resolve_display_name(url_map, stack_name, service_name):
    """Retorna a URL da aplicação; se não houver, cai para 'stack/servico'."""
    name = url_map.get(stack_name) if stack_name else None
    if not name:
        name = f"{stack_name}/{service_name}" if stack_name else service_name
    return name[:100]


def fetch_failed_tasks():
    """Captura falhas de Replicated Jobs (ex: php artisan migrate, drush updb/initupdb)."""
    conn = None
    cursor = None
    try:
        client = docker.from_env()
        tasks = client.api.tasks(filters={"desired-state": "shutdown"})
        url_map = build_stack_url_map(client)
        type_map = build_stack_type_map(client)
        
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
                raw_date = dateutil.parser.parse(timestamp_str) if timestamp_str else None
                data_ocorrencia = adjust_datetime(raw_date)
                
                error_msg = task_status.get("Err", "Erro desconhecido informado pelo Swarm")

                full_service_name = resolve_display_name(url_map, stack_name, service_name)

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
                    resolve_type(type_map, stack_name),
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
        url_map = build_stack_url_map(client)
        type_map = build_stack_type_map(client)

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
                        parsed_dt = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M:%S")
                        data_ocorrencia = adjust_datetime(parsed_dt)
                    except ValueError:
                        data_ocorrencia = adjust_datetime()
                else:
                    data_ocorrencia = adjust_datetime()

                first_line = entry.strip().split('\n')[0]
                error_fingerprint = f"{service_name}:{first_line}:{data_ocorrencia.strftime('%Y-%m-%d-%H')}"
                id_externo = hashlib.md5(error_fingerprint.encode('utf-8')).hexdigest()

                full_service_name = resolve_display_name(url_map, stack_name, service_name)

                mensagem_completa = f"Erro de App (Laravel 500):\n{entry.strip()}"

                insert_query = """
                    INSERT IGNORE INTO alerts (
                        type, id_externo, service_name, data_ocorrencia, mensagem, status_resolucao, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, 'pendente', NOW(), NOW())
                """

                cursor.execute(insert_query, (
                    resolve_type(type_map, stack_name),
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
    Captura erros de Watchdog (severidades 1, 2 e 3) e do Relatório de Status (drush rq --severity=2)
    em cada stack Drupal do Swarm (executando em APENAS UMA réplica por site).
    """
    conn = None
    cursor = None
    try:
        client = docker.from_env()
        conn = get_db_connection()
        cursor = conn.cursor()

        containers = client.containers.list(filters={"status": "running"})
        url_map = build_stack_url_map(client)
        type_map = build_stack_type_map(client)
        drupal_stacks = {}

        for container in containers:
            image_name = container.image.tags[0] if container.image.tags else ""
            container_name = container.name

            if "drupal" not in image_name.lower() and "drupal" not in container_name.lower():
                continue

            if "cron" in container_name.lower() or "initupdb" in container_name.lower():
                continue

            labels = container.labels
            stack_name = labels.get("com.docker.stack.namespace", container_name)

            if stack_name not in drupal_stacks:
                drupal_stacks[stack_name] = container

        severidades_criticas = ["1", "2", "3"]

        for stack_name, container in drupal_stacks.items():
            labels = container.labels
            service_name = labels.get("com.docker.swarm.service.name", container.name)
            full_service_name = resolve_display_name(url_map, stack_name, service_name)

            # ----------------------------------------------------
            # 1. MONITORAMENTO DO WATCHDOG (Severidades 1, 2 e 3)
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

                        id_externo = f"drupal-watchdog-{stack_name}-{wid}"

                        type_str = item.get("type", "drupal")
                        severity_str = item.get("severity", severidade)
                        message_raw = item.get("message", "")
                        date_str = item.get("date", "")
                        location = item.get("location", "N/A")

                        try:
                            parsed_dt = dateutil.parser.parse(date_str) if date_str else None
                            data_ocorrencia = adjust_datetime(parsed_dt)
                        except Exception:
                            data_ocorrencia = adjust_datetime()

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
                            resolve_type(type_map, stack_name, default='drupal'),
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

                            data_ocorrencia = adjust_datetime()
                            id_externo = f"drupal-rq-{stack_name}-{req_key}-{data_ocorrencia.strftime('%Y-%m-%d')}"

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
                                ) VALUES (%s, %s, %s, %s, %s, 'pendente', NOW(), NOW())
                            """

                            cursor.execute(insert_query, (
                                resolve_type(type_map, stack_name, default='drupal'),
                                id_externo,
                                full_service_name,
                                data_ocorrencia,
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
