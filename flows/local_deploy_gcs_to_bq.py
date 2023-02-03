from prefect.deployments import Deployment
from el_gcs_to_bq_param import el_parent_flow


docker_dep = Deployment.build_from_flow(
    flow=el_parent_flow,
    name="local-flow"
)


if __name__ == "__main__":
    docker_dep.apply()