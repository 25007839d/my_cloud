from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateClusterOperator,)
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}
create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=	'ritu-351906',
    cluster_config=CLUSTER_CONFIG,
    region='us-central1',
    cluster_name='radha-a01',
)