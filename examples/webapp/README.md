# Example usage in "production"-style FastAPI app

(All shell examples assume you've `cd`'ed into `examples/webapp`.)

```sh
# terminal A
$ uv run db-start
Starting postgres db in docker...
Started.
```

```sh
# terminal B
$ uv run webapp
INFO:     Started server process [10369]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     127.0.0.1:62109 - "POST /do-work HTTP/1.1" 200 OK
```

```sh
# terminal C
$ uv run worker
INFO:taskiq.worker:Pid of a main process: 10856
INFO:taskiq.worker:Starting 1 worker processes.
INFO:taskiq.process-manager:Started process worker-0 with pid 10858 
```

```sh
# terminal D
$ curl -X localhost:8000/do-work
"no more work, please."%
```

<details>
  <summary>Stop the db</summary>

```sh
# terminal A
$ uv run db-stop
Stopping postgres db in docker...
Stopped.
```
</details>

---

## Problems

### 1. Taskiq requires config to be available at import-time

In prod, I don't really want config in the global scope, but taskiq requires it.

(Why no configuration in a module's global scope? It then requires config to be
available at import-time, not runtime.)

e.g.:

```py
# webapp/worker.py
import asyncio

import taskiq_fastapi
from taskiq.serializers import JSONSerializer

from taskiq_pg import AsyncpgBroker, AsyncpgResultBackend

# declaring result backend & broker in global scope, but they requires configuration
result_backend: AsyncpgResultBackend[object] = AsyncpgResultBackend(
    dsn=os.environ["DB_DSN"], # this has to be known on import
    serializer=JSONSerializer(),
)
broker = AsyncpgBroker(dsn=os.environ["DB_DSN"]).with_result_backend(result_backend)

taskiq_fastapi.init(broker, "webapp.webapp:make_app")

@broker.task()
async def so_much_effort() -> None:
    """Worker task."""
    await asyncio.sleep(1)
```

This causes mypy, pytest, or any scripts to fail if they import from `webapp.worker` and DB_DSN isn't set.

This is undesirable because generally we'd want to separate configuration from code.

To do this, I use pydantic-settings and that-depends, which give me a dedicated place to
manage my config and "inject"/"provide" it to my code at runtime.

I haven't yet figured out how to get around this in taskiq.

### 2. Logs from broker workers are lost

This might be me doing something wrong, but at the moment I can't figure out how to get the logs
for taskiq broker worker processes to output to stdout/stderr.
