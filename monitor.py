import os
import time
from datetime import datetime
import dateutil.parser
import docker
import mysql.connector

# Configurações do Banco via Variáveis de Ambiente (padrões genéricos para repo público)
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
    client = docker.from_env()
    
    # Consulta tarefas com estado desejado de desligamento/falha via APIClient (client.api)
    tasks = client.api.tasks(filters={"desired-state": "shutdown"})
    
    conn = get_db_connection()
    cursor = conn.cursor()

    for task in tasks:
        task_status = task.get("Status", {})
        state = task_status.get("State")
        
        # Filtra apenas falhas reais
        if state in ["failed", "rejected"]:
            task_id = task.get("ID")
            timestamp_str = task_status.get("Timestamp")
            data_ocorrencia = dateutil.parser.parse(timestamp_str) if timestamp_str else datetime.utcnow()
            
            error_msg = task_status.get("Err", "Erro desconhecido informado pelo Swarm")
            
            service_id = task.get("ServiceID")
            service_name = "desconhecido"
            stack_name = ""
            
            try:
                service = client.services.get(service_id)
                service_name = service.name
                labels = service.attrs.get("Spec", {}).get("Labels", {})
                stack_name = labels.get("com.docker.stack.namespace", "")
            except Exception:
                pass

            # Formata como "stack/servico" ou apenas "servico" (respeitando limite de 100 caracteres)
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

            mensagem_completa = f"Erro Swarm: {error_msg}\n\n--- Últimos Logs ---\n{logs}"

            # Inserção no MariaDB/MySQL
            # Usa INSERT IGNORE para ignorar a inserção se id_externo já existir na tabela
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

if __name__ == "__main__":
    print("Iniciando monitoramento de falhas do Docker Swarm...")
    while True:
        try:
            fetch_failed_tasks()
        except Exception as e:
            print(f"Erro durante a execução do monitor: {e}")
        time.sleep(POLL_INTERVAL)