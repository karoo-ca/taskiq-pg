"""Helper script to run a postgres db using the same settings as the webapp & worker."""

import shutil
import subprocess
from textwrap import indent, wrap

from that_depends import Provide, container_context, inject

from webapp.dependencies import Dependencies
from webapp.settings import Settings


def _handle_run_result(completed_process: subprocess.CompletedProcess[bytes]) -> None:
    prefix = ">  "
    width = shutil.get_terminal_size().columns - len(prefix)
    if completed_process.returncode != 0:
        print("Problem starting!")  # noqa: T201
        for line in wrap(completed_process.stdout.decode(), width):
            print(indent(line, prefix))  # noqa: T201
        for line in wrap(completed_process.stderr.decode(), width):
            print(indent(line, prefix))  # noqa: T201
        raise SystemExit(1)


@container_context()
@inject
def start(settings: Settings = Provide[Dependencies.settings]) -> None:
    """Start postgres db in docker."""
    print("Starting postgres db in docker...")  # noqa: T201
    result = subprocess.run(  # noqa: S603
        [  # noqa: S607
            "docker",
            "run",
            "-d",
            "--name",
            "taskiq-pg-example",
            "-p",
            f"{settings.PGPORT}:5432",
            "-e",
            f"POSTGRES_USER={settings.PGUSER}",
            "-e",
            f"POSTGRES_PASSWORD={settings.PGPASSWORD.get_secret_value()}",
            "-e",
            f"POSTGRES_DB={settings.PGDATABASE}",
            "postgres:16",
        ],
        check=False,
        capture_output=True,
    )
    _handle_run_result(result)
    print("Started.")  # noqa: T201


def stop() -> None:
    """Stop postgres db in docker."""
    print("Stopping postgres db in docker...")  # noqa: T201
    result = subprocess.run(  # noqa: S603
        [  # noqa: S607
            "docker",
            "stop",
            "taskiq-pg-example",
        ],
        check=False,
        capture_output=True,
    )
    _handle_run_result(result)
    result = subprocess.run(  # noqa: S603
        [  # noqa: S607
            "docker",
            "rm",
            "--volumes",
            "taskiq-pg-example",
        ],
        check=False,
        capture_output=True,
    )
    _handle_run_result(result)
    print("Stopped.")  # noqa: T201
