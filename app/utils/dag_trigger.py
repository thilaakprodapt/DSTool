import subprocess
import time

SCHEDULER = "airflow-scheduler-1"   # <-- Update to match your docker-compose

def run_cmd(cmd):
    """Run shell commands and return output"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip(), result.stderr.strip()

def dag_exists(dag_id):
    """Check if DAG exists in Airflow"""
    out, _ = run_cmd(f"docker exec {SCHEDULER} airflow dags list")
    return dag_id in out

def is_dag_paused(dag_id):
    """Check if DAG is paused"""
    out, _ = run_cmd(f"docker exec {SCHEDULER} airflow dags list --output table")
    for line in out.split("\n"):
        if dag_id in line:
            return "True" in line or "paused" in line.lower()
    return False

def unpause_dag(dag_id):
    """Unpause the DAG"""
    print(f"Unpausing DAG: {dag_id}")
    run_cmd(f"docker exec {SCHEDULER} airflow dags unpause {dag_id}")

def trigger_dag(dag_id):
    """Trigger the DAG"""
    print(f"Triggering DAG: {dag_id}")
    run_cmd(f"docker exec {SCHEDULER} airflow dags trigger {dag_id}")

def wait_for_dag_and_trigger(dag_id):
    """Wait for DAG → unpause if needed → trigger"""
    print(f"Checking for DAG '{dag_id}'...")

    # Wait up to 2 minutes
    for _ in range(40):
        if dag_exists(dag_id):
            print(f"✅ DAG '{dag_id}' detected in Airflow!")
            break
        print("DAG not found. Waiting...")
        time.sleep(3)
    else:
        print("⛔ Timeout: DAG not detected.")
        return False

    # Check paused state
    if is_dag_paused(dag_id):
        print(f"⚠️ DAG '{dag_id}' is paused.")
        unpause_dag(dag_id)
    else:
        print("✔ DAG already unpaused.")

    # Trigger DAG
    trigger_dag(dag_id)
    print("🎉 DAG triggered successfully!")
    return True