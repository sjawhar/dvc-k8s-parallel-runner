import logging
import pathlib
from typing import Optional, Tuple

import click

logger = logging.getLogger(__name__)


def _setup_logging(ctx: click.Context, param: click.Option, value: bool):
    if param.name != "verbose":
        return

    logging.basicConfig()
    logging.getLogger(__package__).setLevel(logging.DEBUG if value else logging.INFO)


@click.command()
@click.argument(
    "WORKLIST_FILE",
    type=click.Path(
        exists=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.argument("IDX_WORK_ITEM", type=int, envvar="JOB_COMPLETION_INDEX")
@click.option(
    "--output-dir",
    type=click.Path(
        file_okay=False,
        writable=True,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
    default="results",
)
@click.option(
    "--project-dir",
    type=click.Path(
        exists=True,
        file_okay=False,
        readable=True,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
    default=".",
)
@click.option("--fetch/--no-fetch", default=True)
@click.option("-r", "--retries", type=int, default=0)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    callback=_setup_logging,
    expose_value=False,
)
def neuromancer(
    worklist_file: pathlib.Path,
    idx_work_item: int,
    output_dir: pathlib.Path,
    project_dir: pathlib.Path,
    fetch: bool,
    retries: int,
):
    from neuromancer import repro

    attempt_file = output_dir / f"job-{idx_work_item}.log"
    try:
        attempt = int(attempt_file.read_text()) + 1
    except FileNotFoundError:
        attempt = 1
    attempt_file.parent.mkdir(exist_ok=True, parents=True)
    attempt_file.write_text(str(attempt))

    try:
        repro.neuromancer(
            worklist_file,
            idx_work_item,
            output_dir,
            project_dir=project_dir,
            fetch=fetch,
        )
    except Exception as error:
        if attempt < retries:
            raise

        logger.error(
            "Failed %d attempts for session %d, stopping",
            attempt,
            idx_work_item,
            exc_info=error,
        )

    attempt_file.unlink()


@click.command()
@click.argument("IMAGE")
@click.argument("PIPELINES", nargs=-1)
@click.option("-p", "--provider", default="iterative/iterative")
@click.option("-m", "--machine", default="4-10240")
@click.option("-d", "--disk-size", type=int, default=20)
@click.option("-w", "--workers", type=int, default=4)
@click.option("-s", "--storage-class", default="local-path")
@click.option("-e", "--env", nargs=2, multiple=True)
@click.option("--nfs", "nfs_volumes", nargs=3, multiple=True)
@click.option("--fetch/--no-fetch", default=True)
@click.option("-t", "--timeout", type=float, default=48)
@click.option("-r", "--retries", type=int, default=0)
@click.option(
    "--project-dir",
    type=click.Path(
        exists=True,
        file_okay=False,
        readable=True,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
    default=".",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    callback=_setup_logging,
    expose_value=False,
)
@click.option(
    "--script",
    "script_file",
    type=click.Path(
        exists=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.option("--tag", "tags", multiple=True)
@click.option("--node-selector", "node_selectors", multiple=True)
@click.option("--cleanup/--no-cleanup", default=True)
def wintermute(
    image: str,
    pipelines: Tuple[str, ...],
    provider: str,
    machine: str,
    disk_size: int,
    workers: int,
    timeout: float,
    retries: int,
    storage_class: str,
    env: Tuple[Tuple[str, str]],
    nfs_volumes: Tuple[Tuple[str, str, str], ...],
    fetch: bool,
    project_dir: pathlib.Path,
    script_file: Optional[pathlib.Path],
    cleanup: bool,
    tags: Tuple[str, ...],
    node_selectors: Tuple[str, ...],
):
    from neuromancer import queue

    queue.wintermute(
        image,
        timeout,
        pipelines=pipelines or None,
        project_dir=project_dir,
        machine=machine,
        disk_size=disk_size,
        env=dict(env),
        nfs_volumes=nfs_volumes,
        fetch=fetch,
        provider=provider,
        storage_class=storage_class,
        workers=workers,
        retries=retries,
        script=script_file.read_text() if script_file else None,
        cleanup=cleanup,
        node_selectors=node_selectors,
        tags=dict((tag.split("=") for tag in tags)),
    )
