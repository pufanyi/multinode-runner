# multinode-runner

`multinode-runner` is a lightweight orchestration tool that helps debug multi machine
setups. Every node runs the `server` command once to establish the cluster, while the
interactive `client` command can submit shell commands, watch real-time logs, and stop
jobs on all nodes at once.

## Installation

Install the package on every machine (master and workers) using `pip`:

```bash
pip install .
```

## Usage

1. **Start the servers**

   Run the same command on every node. The first machine that cannot reach an existing
   master automatically becomes the master and starts accepting connections.

   ```bash
   multinode-runner server --master <MASTER_IP> --port 9000
   ```

   Use `--bind` if the master has to listen on a specific network interface.

2. **Launch the client UI**

   From any machine with network access to the master:

   ```bash
   multinode-runner client --master <MASTER_IP> --port 9000
   ```

   The interactive shell supports the following commands:

   - `submit <command>` – run a shell command on every connected worker
   - `stop <task_id>` – stop the command identified by `task_id`
   - `tasks` – list all known tasks and their state per worker
   - `workers` – display the currently connected workers
   - `logs <task_id>` – show the stored logs for a task
   - `quit` – exit the client

## Design overview

- **Master election** – the first node that cannot reach an existing master starts a
  local `MasterServer` and listens for new connections.
- **Workers** – every server instance also acts as a worker and connects to the master
  to execute commands.
- **Clients** – interactive terminals communicate with the master to dispatch commands
  and receive log updates in real time.
- **Protocol** – control traffic uses newline-delimited JSON sent over TCP sockets.

All console output is in English, matching the source code comments and documentation.
