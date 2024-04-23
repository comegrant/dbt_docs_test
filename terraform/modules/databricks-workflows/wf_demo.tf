resource "databricks_job" "demo_job" {
  name = "Demo job"

  job_cluster {
    job_cluster_key = "job_cluster"

    new_cluster {
      num_workers = 1
      spark_version = data.databricks_spark_version.latest_lts.id
      node_type_id  = data.databricks_node_type.smallest.id
    }
    
  }

  task {
    task_key = "bronze_cms_full"
    notebook_task {
        notebook_path = "/Workspace/Users/anna.broyn@cheffelo.com/Development/projects/data-model/01. bronze/bronze_cms"
    }
    job_cluster_key = "job_cluster"
  }

  task {
    task_key = "gold_dim_company"
    notebook_task {
        notebook_path = "/Workspace/Users/anna.broyn@cheffelo.com/Development/projects/data-model/03. gold/gold_dim_company"
    }
    dynamic "depends_on" {
      for_each = ["bronze_cms_full"]
      content {
        task_key = depends_on.value
      }
    }
    job_cluster_key = "job_cluster"
  }
}