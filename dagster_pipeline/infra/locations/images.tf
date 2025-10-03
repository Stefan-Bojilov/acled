# Push the Docker image to ECR
resource "docker_registry_image" "dagster_acled" {
  name = docker_image.dagster_acled.name
  
  triggers = {
    img_digest = docker_image.dagster_acled.repo_digest
  }
  
  keep_remotely = true
}

# Build the Docker image
resource "docker_image" "dagster_acled" {
  name = "${module.ecr.repository_url}:${var.image_tag}"
  
  build {
    context = "../../dagster_pipeline"  
    dockerfile = "Dockerfile"
  }
  
  triggers = {
    # Rebuild when any file in dagster_pipeline changes
    dir_sha1 = sha1(join("", [
      for f in fileset("${path.module}/../../dagster_pipeline", "**") :
      filesha1("${path.module}/../../dagster_pipeline/${f}")
    ]))
  }
  
  keep_locally = true
}