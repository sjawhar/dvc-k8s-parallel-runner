from __future__ import annotations

import concurrent.futures
import datetime
import json
import logging
import pathlib
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)

import dvc.dvcfile
import dvc.parsing
import dvc.repo
import dvc.stage
import kubernetes.client
import kubernetes.config
import kubernetes.watch
import urllib3.exceptions
import yaml

from neuromancer import path

if TYPE_CHECKING:
    from dvc.repo import Repo as DvcRepo
    from dvc.stage.loader import StageLoader as DvcStageLoader
    from kubernetes.client import V1Job, V1JobStatus

logger = logging.getLogger(__name__)

_OUTPUT_DIR_NAME = "results"
_TF_OUTPUT_JOB_NAME = "job_name"
_TF_OUTPUT_LOGS = "logs"
_TF_OUTPUT_EVENTS = "events"


def wintermute(
    image: str,
    timeout: float,
    pipelines: Optional[Iterable[str]] = None,
    project_dir: Optional[pathlib.Path] = None,
    cleanup: bool = True,
    **kwargs: Any,
):
    """Plans and orchestrates parallel execution of DVC pipelines in Kubernetes

    A worklist is built by gathering all the stages for which each session needs
    to be reproduced, and then assigning a worker to reproduce each session.

    Assumes that pipelines follow a map-reduce pattern, where early stages are
    parametrized over sessions (foreach: ${sessions}) and later stages
    aggregate/summarize the results for each session. This is true of many/most
    of Kernel's pipelines (e.g. metrics and studies repos). Correct behavior is
    not guaranteed if the DAG contains multiple merges and splits—e.g. stages A
    and B are parametrized, then C depends on the results of all A@* and B@*,
    and then D depends on C AND is also parametrized.

    Parameters
    ----------
    image : str
        Name of docker image to be used by parallel pods/workers
    timeout : float
        Number of hours to wait for completion
    pipelines : Iterable[str] | None, optional
        List of pipeline names to reproduce. A pipeline's name is the relative
        path to the directory containing its dvc.yaml file. None == reproduce
        all pipelines
    project_dir : pathlib.Path | None, optional
        Directory containing the DVC project to reproduce. If not provided, the
        current directory is used.
    cleanup : bool, optional
        Whether to destroy the k8s job and output directory on completion.
        Default is True
    kwargs : Any
        Used to populate the Terraform template (see `get_terraform_hcl`)
    """
    dvc_repo = dvc.repo.Repo(root_dir=project_dir)
    project_dir = pathlib.Path(dvc_repo.root_dir)
    session_stages = _get_session_stages(dvc_repo, project_dir, pipelines=pipelines)

    if not session_stages:
        raise ValueError(f"No stages found for pipelines {pipelines}")

    output_dir = project_dir / _OUTPUT_DIR_NAME
    output_dir.mkdir(exist_ok=True, parents=True)
    with tempfile.TemporaryDirectory() as tmp_dir, path.run_in_dir(tmp_dir):
        logger.info("Running in %s", tmp_dir)

        worklist_file = project_dir / "worklist.txt"
        worklist_file.write_text(
            "\n".join(
                ",".join([session_id, *stages])
                for session_id, stages in session_stages.items()
            )
        )

        terraform_hcl = _get_terraform_hcl(
            completions=len(session_stages),
            image=image,
            project_dir=project_dir,
            worklist_file=str(worklist_file.relative_to(project_dir)),
            **kwargs,
        )

        terraform_json = json.dumps(terraform_hcl, indent=2)
        logger.debug("main.tf.json contents:\n%s", terraform_json)
        with open("main.tf.json", "w") as tf_file:
            tf_file.write(terraform_json)

        _run(timeout=timeout, cleanup=cleanup)

        _handle_outputs(output_dir, {*session_stages}, project_dir)

    if cleanup:
        worklist_file.unlink()
        shutil.rmtree(output_dir)

    logger.info("Parallel execution complete")


def _get_session_stages(
    dvc_repo: DvcRepo,
    project_dir: pathlib.Path,
    pipelines: Optional[Iterable[str]] = None,
) -> Dict[str, Set[str]]:
    "Get the set of stages for which each session needs to be reproduced"

    get_pipeline_name: Callable[[pathlib.Path], str] = lambda pipeline_file: "/".join(
        pipeline_file.parent.relative_to(project_dir).parts
    )

    pipelines = {
        *(
            pipelines
            or [
                get_pipeline_name(pipeline_file)
                for pipeline_file in project_dir.rglob("dvc.yaml")
            ]
        )
    }

    logger.info("Gettings stages for pipelines: %s", pipelines)

    session_stages: Dict[str, Set[str]] = defaultdict(set)
    stage_loaders: Dict[str, DvcStageLoader] = {}
    for stage in cast(List[dvc.stage.Stage], dvc_repo.stages):
        if not isinstance(stage.dvcfile, dvc.dvcfile.PipelineFile):
            continue

        pipeline_file: str = cast(str, stage.path_in_repo)
        pipeline_name = get_pipeline_name(project_dir / pipeline_file)
        if pipeline_name not in pipelines:
            logger.debug("%s not in included pipelines, skipping", pipeline_name)
            continue

        stage_name, is_parametrized, session_id = cast(str, stage.name).rpartition(
            dvc.parsing.JOIN
        )
        if not is_parametrized:
            logger.debug("Stage %s is not parametrized, skipping", stage.name)
            continue

        if pipeline_file not in stage_loaders:
            stage_loaders[pipeline_file] = stage.dvcfile.stages
        stage_loader = stage_loaders[pipeline_file]
        if (
            stage_data := cast(
                Dict[str, Any],
                stage_loader.stages_data.get(stage_name, None),
            )
        ) is None or "sessions" not in stage_data.get(dvc.parsing.FOREACH_KWD, ""):
            logger.debug(
                "Stage %s is not parametrized over sessions, skipping", stage_name
            )
            continue

        full_stage_name = f"{pipeline_file}:{stage_name}"
        logger.debug("Adding stage %s to session %s", full_stage_name, session_id)
        session_stages[session_id].add(full_stage_name)

    logger.info("Collected stages for %d sessions", len(session_stages))
    return dict(session_stages)


def _get_terraform_hcl(
    *,
    completions: int,
    image: str,
    project_dir: pathlib.Path,
    machine: str = "4-10240",
    nfs_volumes: Tuple[Tuple[str, str, str], ...] = (),
    fetch: bool = True,
    provider: str = "iterative/iterative",
    retries: int = 0,
    worklist_file: str = "worklist.txt",
    storage_class: str = "local-path",
    workers: int = 4,
    disk_size: int = 20,
    env: Optional[Dict[str, str]] = None,
    script: Optional[str] = None,
    node_selectors: Optional[List[str]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    if script is None:
        script = " ".join(
            [
                "neuromancer",
                "--verbose",
                f"--output-dir={_OUTPUT_DIR_NAME}",
                f"--retries={retries}",
                f"--{'' if fetch else 'no-'}fetch",
                worklist_file,
                "$${JOB_COMPLETION_INDEX}",
            ]
        )

    task: Dict[str, Any] = {
        "cloud": "k8s",
        "image": image,
        "machine": machine,
        "disk_size": disk_size,
        "completions": completions,
        "parallelism": min(workers, completions),
        "storage": {
            "workdir": f"{storage_class}:1:{project_dir}",
            "output": _OUTPUT_DIR_NAME,
        },
        "script": script,
    }
    if env:
        task["environment"] = env
    if node_selectors:
        task["region"] = ",".join(node_selectors)
    if tags:
        task["tags"] = tags

    for server, server_path, mount_path in nfs_volumes:
        cast(List[Dict[str, str]], task.setdefault("nfs_volume", [])).append(
            {
                "server": server,
                "server_path": server_path,
                "mount_path": mount_path,
            }
        )

    task_id = "_".join(
        [
            re.sub(r"[^a-z0-9_]", "", project_dir.name.lower()),
            uuid.uuid4().hex,
        ]
    )
    return {
        "terraform": {
            "required_providers": {
                "iterative": {
                    "source": provider,
                }
            }
        },
        "provider": {
            "iterative": {},
        },
        "resource": {
            "iterative_task": {
                task_id: task,
            }
        },
        "output": {
            _TF_OUTPUT_JOB_NAME: {
                "value": f"${{iterative_task.{task_id}.id}}",
            },
            _TF_OUTPUT_LOGS: {
                "value": f'${{try(join("\\n", iterative_task.{task_id}.logs), "")}}',
            },
            _TF_OUTPUT_EVENTS: {
                "value": f'${{try(join("\\n", iterative_task.{task_id}.events), "")}}',
            },
        },
    }


def _get_terraform_output(
    output_name: str, refresh: bool = False, timeout: Optional[float] = None
) -> str:
    _run: Callable[
        [List[str]], subprocess.CompletedProcess[str]
    ] = lambda cmd: subprocess.run(
        cmd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )

    if refresh:
        _run(["terraform", "refresh"])

    return _run(
        ["terraform", "output", "-no-color", "-raw", output_name]
    ).stdout.strip()


def _run(timeout: float = 24 * 365, cleanup: bool = True):
    kubernetes.config.load_config()
    subprocess.run(["terraform", "init", "-no-color"], check=True)

    state_file = pathlib.Path("terraform.tfstate")

    def _change_tf_state(*command: str):
        subprocess.run(
            [
                "terraform",
                *command,
                "-no-color",
                "-auto-approve",
                f"-state={state_file}",
            ],
            check=True,
        )

    try:
        _change_tf_state("apply")

        job_name = _get_terraform_output(_TF_OUTPUT_JOB_NAME)
        logger.debug("job name is %s", job_name)

        with kubernetes.client.ApiClient() as client:
            kube_client = kubernetes.client.BatchV1Api(client)
        namespace: str = (
            kube_client.list_job_for_all_namespaces(
                field_selector=f"metadata.name={job_name}",
                limit=1,
            )
            .items[0]
            .metadata.namespace
        )
        logger.debug("namespace is %s", namespace)

        start = datetime.datetime.now()
        deadline = start + datetime.timedelta(hours=timeout)
        log: Callable[
            ..., None
        ] = lambda message, *args, level=logging.INFO, **kwargs: logger.log(
            level,
            f"%s - {message}",
            datetime.datetime.now() - start,
            *args,
            **kwargs,
        )

        def _print_k8s_job_logs():
            refresh = True
            for output in [_TF_OUTPUT_LOGS, _TF_OUTPUT_EVENTS]:
                try:
                    log(
                        "%s:\n%s",
                        output,
                        _get_terraform_output(output, refresh=refresh, timeout=60),
                        level=logging.DEBUG,
                    )
                except (
                    subprocess.CalledProcessError,
                    subprocess.TimeoutExpired,
                ) as error:
                    error_message = error.stdout
                    log("Failed to get %s: %s", output, error_message)
                    if isinstance(error, subprocess.TimeoutExpired) or (
                        "ResourceExhausted" in error_message
                        and "received message larger than max" in error_message
                    ):
                        log(
                            "Stopping log polling, logs are too large",
                            level=logging.WARNING,
                        )
                        return False

                refresh = False

            return True

        completions: int = 0
        num_active: int = 0
        num_failed: int = 0
        num_succeeded: int = 0
        log_counts = lambda: log(
            "%d/%d succeeded, %d failed, %d active",
            num_succeeded,
            completions,
            num_failed,
            num_active,
        )

        is_complete = False
        print_logs = True
        http_retries = 0
        resource_version: Optional[int] = None
        while not is_complete and datetime.datetime.now() < deadline:
            if print_logs:
                print_logs = _print_k8s_job_logs()
            if completions:
                log_counts()

            try:
                for event in kubernetes.watch.Watch().stream(
                    kube_client.list_namespaced_job,
                    namespace,
                    field_selector=f"metadata.name={job_name}",
                    resource_version=resource_version,
                    timeout_seconds=300,
                ):
                    http_retries = 0
                    if print_logs:
                        print_logs = _print_k8s_job_logs()

                    job: V1Job = event["object"]
                    resource_version = cast(
                        kubernetes.client.V1ObjectMeta, job.metadata
                    ).resource_version

                    status: V1JobStatus = job.status
                    completions = cast(
                        kubernetes.client.V1JobSpec, job.spec
                    ).completions
                    num_active = status.active or 0
                    num_failed = status.failed or 0
                    num_succeeded = status.succeeded or 0
                    log_counts()

                    if status.conditions is not None:
                        log(
                            "conditions:\n%s",
                            [
                                condition.to_dict()
                                for condition in cast(
                                    List[kubernetes.client.V1JobCondition],
                                    status.conditions,
                                )
                            ],
                            level=logging.DEBUG,
                        )

                    if status.completion_time is not None:
                        is_complete = True
                        break
                    elif (num_failed >= completions) or (
                        num_failed >= 5 and num_succeeded == 0
                    ):
                        raise RuntimeError("Job failure detected, quitting early")
            except urllib3.exceptions.HTTPError:
                sleep_time = int(2**http_retries)
                logger.exception(f"Received HTTPError, sleeping for {sleep_time:d} sec")
                time.sleep(sleep_time)
                http_retries += 1
                if http_retries >= 5:
                    raise

        if not is_complete:
            raise TimeoutError(f"Job did not complete in {timeout} seconds")
    finally:
        if cleanup:
            logger.info("Deleting job resources")
            _change_tf_state("destroy", "-refresh=false")
        else:
            logger.info("Skipping job cleanup. State file:\n%s", state_file.read_text())


def _handle_outputs(
    output_dir: pathlib.Path, sessions: Set[str], project_dir: pathlib.Path
):
    "Scan output_dir for results from workers to merge back into the main repo"

    logger.info("Processing parallel results...")

    dvc_locks: Dict[pathlib.Path, Dict[str, Any]] = defaultdict(dict)

    def _handle_output(session_output_file: pathlib.Path):
        relative_path = session_output_file.relative_to(output_dir)
        logger.debug("Processing %s", relative_path)
        session_id, *file_parts = relative_path.parts
        if session_id not in sessions:
            logger.debug("%s not in included sessions, skipping...", session_id)
            return

        file_dst = project_dir.joinpath(*file_parts)
        file_dst.parent.mkdir(exist_ok=True, parents=True)
        if session_output_file.name == "dvc.lock":
            logger.debug("Reading lock from %s", relative_path)
            dvc_locks[file_dst][session_id] = yaml.safe_load(
                session_output_file.read_text()
            )["stages"]
            return

        logger.debug("Copying %s", file_dst)
        shutil.copy(session_output_file, file_dst)

    with concurrent.futures.ThreadPoolExecutor() as pool:
        [
            *pool.map(
                _handle_output,
                (file for file in output_dir.rglob("*") if not file.is_dir()),
            )
        ]
        num_locks = len(dvc_locks)
        logger.info("%d lock files to update", num_locks)
        if num_locks == 0:
            return

        done, _ = concurrent.futures.wait(
            [
                pool.submit(_merge_session_locks, lock_file, session_locks)
                for lock_file, session_locks in dvc_locks.items()
            ],
            return_when=concurrent.futures.FIRST_EXCEPTION,
        )
        if exception := next(
            (exception for future in done if (exception := future.exception())),
            None,
        ):
            raise exception


def _merge_session_locks(lock_file: pathlib.Path, session_locks: Dict[str, Any]):
    dvc_lock: Dict[str, Any] = {"schema": "2.0", "stages": {}}
    if lock_file.exists():
        dvc_lock = yaml.safe_load(lock_file.read_text())

    for session_lock in session_locks.values():
        dvc_lock["stages"].update(session_lock)
    lock_file.parent.mkdir(exist_ok=True, parents=True)
    lock_file.write_text(yaml.safe_dump(dvc_lock, sort_keys=False))
    logger.info(
        "Updated %d of %d stages in %s",
        len(session_locks),
        len(dvc_lock["stages"]),
        lock_file,
    )
