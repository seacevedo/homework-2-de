from prefect.deployments import Deployment
from prefect.filesystems import GitHub


if __name__ == "__main__":
    github_block = GitHub.load("zoom-github")
    github_block.get_directory("flows")
    from flows.el_web_to_gcs_param import el_parent_flow
    docker_dep = Deployment.build_from_flow(
        flow=el_parent_flow,
        name="local-flow-github"
     )
    docker_dep.apply()