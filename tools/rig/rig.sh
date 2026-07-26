#!/usr/bin/env bash
#
# rig.sh — orchestrator for the real-network measurement rig (docs/scalability-program.md
# section 2). Boots/tears down N RigSilo processes (tools/Spiceport.RigSilo) as real, separate
# OS processes on real loopback sockets, and drives the co-placement A/B procedure over them
# with the Bench remote-* scenarios (tools/Spiceport.Bench).
#
# This is an attended, manual tool — same standing as the `zed` smoke test. It is never invoked
# from tests/. Nothing it does may run inside the automated suite.
#
# Port allocation scheme (deterministic, silo index i = 0..N-1, all on 127.0.0.1):
#   silo-port    = 11500 + i   (Orleans silo-to-silo)
#   gateway-port = 31500 + i   (Orleans client gateway)
#   grpc-port    =  8500 + i   (authzed.api.v1, HTTP/2 h2c)
#   http-port    =  8580 + i   (rig endpoints: /healthz, /rig/metrics, /rig/reset)
# Silo 0's silo-port (11500) is every silo's --primary-silo-port, i.e. silo 0 is always the
# Orleans primary. Silo counts beyond ~80 would need the ranges above to be widened; that has
# never been exercised and isn't allocated for speculatively.
#
# State layout, entirely OUTSIDE the repo (override the root with SPICEPORT_RIG_HOME):
#   $SPICEPORT_RIG_HOME/                default: $HOME/.spiceport-rig
#     rig.pid                           one line per live silo: pid idx siloPort gwPort grpcPort httpPort clusterId
#     rig.pid.partial                   same format, written during the spawn loop; down/status/up
#                                       consult it too so an interrupt mid-spawn cannot orphan silos
#     pg.cid                            container id of the durable-arm Postgres (present only for
#                                       `up --durable` clusters; down removes the container, never
#                                       the data volume)
#     logs/silo-<i>.log                 stdout+stderr of each silo process
#     results/*.json                    `ab` output (--json paths handed to Bench); never in the repo
#
# Durable arm (`up N --durable`): starts a Postgres container (spiceport-rig-pg, port
# SPICEPORT_RIG_PG_PORT default 15432, data on the named volume spiceport-rig-pgdata), applies the
# vendored Orleans DDL on a fresh volume, and passes ConnectionStrings__OrleansStorage to every
# silo, switching AddDatastoreGrainStorage to durable AdoNet storage. The data VOLUME survives
# `down` (recovery runs reboot over it); pass --fresh-data to `up` to drop it first. A loopback
# container prices durability I/O (fsync, serialization), not database-network RTT — numbers stay
# relative, A/B between arms.
#
# Commands:
#   rig.sh up N [--co-locate=on|off] [--durable] [--fresh-data]
#                                       boot an N-silo cluster (default --co-locate=on, the
#                                       production default; rig.sh ab drives both arms explicitly).
#                                       --durable backs the datastore with the Postgres container;
#                                       --fresh-data drops its data volume first (requires --durable)
#   rig.sh down                         tear down the recorded cluster (idempotent)
#   rig.sh status                       live pids + healthz probe per recorded silo
#   rig.sh ab [--silos=N] [--trials=T] [bench flags...]
#                                       co-placement A/B: for each arm (off, then on), T fresh
#                                       up/remote-check/down cycles; passes through flags
#                                       (--seed/--duration/--warmup/world-shape/...) to
#                                       remote-check. Results land under results/, never in the repo.

set -u

# ---- paths -------------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RIGSILO_PROJECT="$REPO_ROOT/tools/Spiceport.RigSilo/Spiceport.RigSilo.csproj"
DLL_PATH="$REPO_ROOT/tools/Spiceport.RigSilo/bin/Release/net10.0/Spiceport.RigSilo.dll"
BENCH_PROJECT="$REPO_ROOT/tools/Spiceport.Bench/Spiceport.Bench.csproj"

STATE_DIR="${SPICEPORT_RIG_HOME:-$HOME/.spiceport-rig}"
PIDFILE="$STATE_DIR/rig.pid"
LOG_DIR="$STATE_DIR/logs"
RESULTS_DIR="$STATE_DIR/results"

# ---- port scheme --------------------------------------------------------------------------

# Overridable because the defaults can collide with other local services (8500 is notably
# Consul's HTTP API port); check_ports_free would refuse cleanly, but an override beats editing
# the script.
BASE_SILO_PORT="${SPICEPORT_RIG_SILO_BASE:-11500}"
BASE_GATEWAY_PORT="${SPICEPORT_RIG_GATEWAY_BASE:-31500}"
BASE_GRPC_PORT="${SPICEPORT_RIG_GRPC_BASE:-8500}"
BASE_HTTP_PORT="${SPICEPORT_RIG_HTTP_BASE:-8580}"

UP_TIMEOUT_SECS=60
DOWN_WAIT_ITERATIONS=20   # 20 * 0.5s = 10s bounded wait before KILL

# Durable arm: the Postgres container backing AddDatastoreGrainStorage's AdoNet path.
PG_PORT="${SPICEPORT_RIG_PG_PORT:-15432}"
PG_CONTAINER="spiceport-rig-pg"
PG_VOLUME="spiceport-rig-pgdata"
PG_IMAGE="postgres:17-alpine"
PG_USER="spiceport"
PG_DB="spiceport"
PG_CIDFILE_NAME="pg.cid"
# The vendored Orleans DDL the durability tests apply (Main first: OrleansQuery + versioning;
# then Persistence: OrleansStorage + query rows) — one source of truth, reused here.
ORLEANS_SQL_DIR_REL="tests/Spiceport.Grains.Tests/Durability/OrleansSql"

DEFAULT_AB_SILOS=3
DEFAULT_AB_TRIALS=3

# ---- small helpers -------------------------------------------------------------------------

die() { echo "rig: $*" >&2; exit 1; }
usage_error() { echo "rig: $*" >&2; echo >&2; print_usage >&2; exit 2; }

print_usage() {
  cat <<'EOF'
Usage:
  rig.sh up N [--co-locate=on|off] [--durable] [--fresh-data]
  rig.sh down
  rig.sh status
  rig.sh ab [--silos=N] [--trials=T] [bench flags...]

See the header of tools/rig/rig.sh for the port scheme and state layout.
EOF
}

require_tool() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not on PATH."; }

siloPortAt()   { echo $((BASE_SILO_PORT + $1)); }
gatewayPortAt(){ echo $((BASE_GATEWAY_PORT + $1)); }
grpcPortAt()   { echo $((BASE_GRPC_PORT + $1)); }
httpPortAt()   { echo $((BASE_HTTP_PORT + $1)); }

grpc_endpoints() {
  local n="$1" i out=""
  for ((i = 0; i < n; i++)); do
    [[ -n "$out" ]] && out+=","
    out+="127.0.0.1:$(grpcPortAt "$i")"
  done
  echo "$out"
}

rig_hosts() {
  local n="$1" i out=""
  for ((i = 0; i < n; i++)); do
    [[ -n "$out" ]] && out+=","
    out+="127.0.0.1:$(httpPortAt "$i")"
  done
  echo "$out"
}

# Recorded pids may be stale (crash/reboot) and recycled by the OS onto unrelated processes;
# every liveness/kill decision must therefore verify the pid's command line still names the
# RigSilo DLL, never trust the pidfile alone.
is_rig_silo_pid() {
  local pid="$1" cmdline
  kill -0 "$pid" 2>/dev/null || return 1
  cmdline="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$cmdline" == *"$DLL_PATH"* ]]
}

# Both the committed pidfile and the in-flight .partial one (a TERM/INT during the spawn loop
# leaves silos recorded only in .partial) — teardown and liveness checks must see both.
# Fills RECORDED_PID_FILES; an array (not stdout) so paths with spaces survive.
recorded_pid_files() {
  RECORDED_PID_FILES=()
  [[ -f "$PIDFILE" ]] && RECORDED_PID_FILES+=("$PIDFILE")
  [[ -f "$PIDFILE.partial" ]] && RECORDED_PID_FILES+=("$PIDFILE.partial")
  return 0
}

# ---- up ------------------------------------------------------------------------------------

refuse_if_already_running() {
  local live_pids="" f pid _rest
  recorded_pid_files
  for f in ${RECORDED_PID_FILES[@]+"${RECORDED_PID_FILES[@]}"}; do
    while read -r pid _rest; do
      [[ -n "$pid" ]] || continue
      is_rig_silo_pid "$pid" && live_pids+="$pid "
    done < "$f"
  done
  if [[ -n "$live_pids" ]]; then
    die "a previous run is still live (pid(s): $live_pids). Run 'rig.sh down' first, or 'rig.sh status' to inspect it."
  fi
}

check_ports_free() {
  local n="$1" i port squatting=0
  for ((i = 0; i < n; i++)); do
    for port in "$(siloPortAt "$i")" "$(gatewayPortAt "$i")" "$(grpcPortAt "$i")" "$(httpPortAt "$i")"; do
      local hit
      hit="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
      if [[ -n "$hit" ]]; then
        echo "rig: port $port is already bound:" >&2
        echo "$hit" >&2
        squatting=1
      fi
    done
  done
  if [[ "$squatting" -eq 1 ]]; then
    die "refusing to start: ports above are squatted. Free them, or if they belong to an untracked rig run, kill the pids shown above."
  fi
}

# ---- durable-arm Postgres --------------------------------------------------------------------

pg_cidfile() { echo "$STATE_DIR/$PG_CIDFILE_NAME"; }

# Connection budget, sized deliberately: Npgsql defaults to a 100-connection pool PER PROCESS, so
# N silos can demand N*100 against Postgres' default max_connections=100 — under load the shard-row
# storage reads exhaust the server ("53300: too many clients already") and every write errors. Cap
# the per-silo pool and start the server with headroom; a real deployment needs the same arithmetic
# (or a pooler like pgbouncer) — silos * MaxPoolSize < max_connections.
PG_MAX_POOL_PER_SILO=25
PG_MAX_CONNECTIONS=300

pg_conn_string() {
  echo "Host=127.0.0.1;Port=$PG_PORT;Database=$PG_DB;Username=$PG_USER;Password=$PG_USER;Maximum Pool Size=$PG_MAX_POOL_PER_SILO"
}

# Same refusal discipline as the silos: an already-present container (running or exited) means a
# previous durable run was not torn down — refuse and point at recovery, never adopt or clobber.
refuse_if_pg_present() {
  if [[ -n "$(docker ps -aq --filter "name=^${PG_CONTAINER}$")" ]]; then
    die "Postgres container '$PG_CONTAINER' already exists (previous durable run not torn down). Run 'rig.sh down' first."
  fi
}

pg_up() {
  local fresh="$1"
  require_tool docker
  refuse_if_pg_present

  local hit
  hit="$(lsof -nP -iTCP:"$PG_PORT" -sTCP:LISTEN 2>/dev/null || true)"
  [[ -z "$hit" ]] || { echo "$hit" >&2; die "port $PG_PORT (Postgres) is already bound; free it or set SPICEPORT_RIG_PG_PORT."; }

  if [[ "$fresh" == "1" ]] && docker volume inspect "$PG_VOLUME" >/dev/null 2>&1; then
    echo "rig: --fresh-data: removing data volume $PG_VOLUME..." >&2
    # 'docker rm -f' returns before the container's mounts are fully released, so an immediate
    # volume rm can race it ("volume is in use") — bounded retry instead of failing the boot.
    local vol_tries=0
    until docker volume rm "$PG_VOLUME" >/dev/null 2>&1; do
      (( ++vol_tries < 10 )) || die "could not remove volume $PG_VOLUME after ${vol_tries}s (still in use?)."
      sleep 1
    done
  fi

  echo "rig: starting Postgres ($PG_IMAGE, port $PG_PORT, volume $PG_VOLUME)..." >&2
  local cid
  cid="$(docker run -d --name "$PG_CONTAINER" \
    -p "127.0.0.1:$PG_PORT:5432" \
    -v "$PG_VOLUME:/var/lib/postgresql/data" \
    -e POSTGRES_USER="$PG_USER" -e POSTGRES_PASSWORD="$PG_USER" -e POSTGRES_DB="$PG_DB" \
    "$PG_IMAGE" -c "max_connections=$PG_MAX_CONNECTIONS")" || die "docker run failed for $PG_CONTAINER."
  echo "$cid" > "$(pg_cidfile)"

  local waited=0
  until docker exec "$PG_CONTAINER" pg_isready -q -U "$PG_USER" -d "$PG_DB" 2>/dev/null; do
    (( waited < UP_TIMEOUT_SECS )) || { pg_down; die "Postgres did not become ready within ${UP_TIMEOUT_SECS}s."; }
    sleep 1; waited=$((waited + 1))
  done

  # Apply the vendored Orleans DDL exactly once per volume (a reused volume already has it; the
  # scripts are not idempotent). OrleansStorage's presence is the applied marker.
  local applied
  applied="$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc \
    "SELECT to_regclass('public.orleansstorage')" 2>/dev/null | tr -d '[:space:]')"
  if [[ -z "$applied" ]]; then
    echo "rig: applying Orleans DDL (fresh volume)..." >&2
    local sql_file
    for sql_file in "PostgreSQL-Main.sql" "PostgreSQL-Persistence.sql"; do
      docker exec -i "$PG_CONTAINER" psql -q -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 \
        < "$REPO_ROOT/$ORLEANS_SQL_DIR_REL/$sql_file" >/dev/null \
        || { pg_down; die "applying $sql_file failed."; }
    done
  fi
}

# Removes the container (recorded or name-matched), NEVER the data volume — recovery runs depend
# on rebooting over it; 'up --durable --fresh-data' is the explicit way to drop it. Idempotent.
pg_down() {
  if [[ -f "$(pg_cidfile)" ]] || [[ -n "$(docker ps -aq --filter "name=^${PG_CONTAINER}$" 2>/dev/null)" ]]; then
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
    if [[ -n "$(docker ps -aq --filter "name=^${PG_CONTAINER}$" 2>/dev/null)" ]]; then
      echo "rig: warning: container $PG_CONTAINER still present after docker rm -f" >&2
    fi
    rm -f "$(pg_cidfile)"
    echo "rig: postgres container removed (data volume $PG_VOLUME kept)" >&2
  fi
}

wait_all_healthy() {
  local n="$1" i http_port deadline
  deadline=$((SECONDS + UP_TIMEOUT_SECS))
  for ((i = 0; i < n; i++)); do
    http_port="$(httpPortAt "$i")"
    until curl -sf -o /dev/null "http://127.0.0.1:$http_port/healthz"; do
      if (( SECONDS >= deadline )); then
        echo "rig: silo $i (http=127.0.0.1:$http_port) did not become healthy within ${UP_TIMEOUT_SECS}s" >&2
        return 1
      fi
      sleep 0.5
    done
  done
  return 0
}

cmd_up() {
  local n="${1:-}"
  [[ "$n" =~ ^[0-9]+$ && "$n" -ge 1 ]] || usage_error "up: N must be a positive integer, got '${n:-<missing>}'."
  shift || true

  local colocate="on" durable=0 fresh_data=0
  local arg
  for arg in "$@"; do
    case "$arg" in
      --co-locate=on) colocate="on" ;;
      --co-locate=off) colocate="off" ;;
      --durable) durable=1 ;;
      --fresh-data) fresh_data=1 ;;
      *) usage_error "up: unknown argument '$arg' (expected --co-locate=on|off, --durable, --fresh-data)." ;;
    esac
  done
  (( fresh_data == 0 || durable == 1 )) || usage_error "up: --fresh-data requires --durable."

  refuse_if_already_running
  check_ports_free "$n"

  mkdir -p "$STATE_DIR" "$LOG_DIR"

  local conn_string=""
  if (( durable == 1 )); then
    pg_up "$fresh_data"
    conn_string="$(pg_conn_string)"
  fi

  echo "rig: building RigSilo (Release, once, so $n processes do not race the build)..." >&2
  dotnet build -c Release "$RIGSILO_PROJECT" >&2 || die "build failed."
  [[ -f "$DLL_PATH" ]] || die "expected build output not found at $DLL_PATH."

  local cluster_id="spiceport-rig-$$"
  local primary_port
  primary_port="$(siloPortAt 0)"

  local partial="$PIDFILE.partial"
  : > "$partial"

  local i
  for ((i = 0; i < n; i++)); do
    local silo_port gw_port grpc_port http_port log pid
    silo_port="$(siloPortAt "$i")"
    gw_port="$(gatewayPortAt "$i")"
    grpc_port="$(grpcPortAt "$i")"
    http_port="$(httpPortAt "$i")"
    log="$LOG_DIR/silo-$i.log"
    echo "rig: starting silo $i (silo=$silo_port gateway=$gw_port grpc=$grpc_port http=$http_port co-locate=$colocate durable=$durable)..." >&2
    # env-var config: AddDatastoreGrainStorage switches to durable AdoNet storage when
    # ConnectionStrings__OrleansStorage is present; empty means in-memory (the default arm).
    ConnectionStrings__OrleansStorage="$conn_string" \
    nohup dotnet "$DLL_PATH" \
      --silo-port="$silo_port" --gateway-port="$gw_port" --primary-silo-port="$primary_port" \
      --grpc-port="$grpc_port" --http-port="$http_port" --co-locate="$colocate" --cluster-id="$cluster_id" \
      > "$log" 2>&1 &
    pid=$!
    echo "$pid $i $silo_port $gw_port $grpc_port $http_port $cluster_id" >> "$partial"
  done
  mv "$partial" "$PIDFILE"

  echo "rig: waiting for $n silo(s) to report healthy (timeout ${UP_TIMEOUT_SECS}s)..." >&2
  if ! wait_all_healthy "$n"; then
    echo "rig: tearing down what was started (see logs under $LOG_DIR)" >&2
    cmd_down
    exit 1
  fi

  echo "rig: cluster up (silos=$n co-locate=$colocate cluster-id=$cluster_id)"
  echo "rig: --endpoints=$(grpc_endpoints "$n")"
  echo "rig: --rig=$(rig_hosts "$n")"
}

# ---- down ----------------------------------------------------------------------------------

cmd_down() {
  recorded_pid_files
  if [[ -z "${RECORDED_PID_FILES[@]+x}" ]]; then
    # No silos recorded, but a durable-arm Postgres may still linger (e.g. a crash between pg_up
    # and the silo spawn loop) — sweep it before declaring the down a no-op.
    pg_down
    echo "rig: no cluster recorded (down)"
    return 0
  fi

  local pids=()
  local f pid _rest
  for f in "${RECORDED_PID_FILES[@]}"; do
    while read -r pid _rest; do
      [[ -n "$pid" ]] || continue
      if is_rig_silo_pid "$pid"; then
        pids+=("$pid")
      elif kill -0 "$pid" 2>/dev/null; then
        echo "rig: recorded pid $pid is alive but is not a RigSilo process (pid recycled); leaving it alone" >&2
      fi
    done < "$f"
  done

  for pid in ${pids[@]+"${pids[@]}"}; do
    kill -TERM "$pid" 2>/dev/null
  done

  local waited=0 any_alive=1
  while (( waited < DOWN_WAIT_ITERATIONS )); do
    any_alive=0
    for pid in ${pids[@]+"${pids[@]}"}; do
      kill -0 "$pid" 2>/dev/null && any_alive=1
    done
    (( any_alive == 0 )) && break
    sleep 0.5
    waited=$((waited + 1))
  done

  for pid in ${pids[@]+"${pids[@]}"}; do
    if is_rig_silo_pid "$pid"; then
      echo "rig: pid $pid did not exit on TERM within $((DOWN_WAIT_ITERATIONS / 2))s, sending KILL" >&2
      kill -KILL "$pid" 2>/dev/null
    fi
  done
  sleep 0.2

  if pgrep -f "$DLL_PATH" >/dev/null 2>&1; then
    echo "rig: warning: process(es) matching $DLL_PATH are still present:" >&2
    pgrep -fl "$DLL_PATH" >&2
  fi

  rm -f "$PIDFILE" "$PIDFILE.partial"
  pg_down
  echo "rig: down"
}

# ---- status --------------------------------------------------------------------------------

cmd_status() {
  if command -v docker >/dev/null 2>&1 && [[ -n "$(docker ps -aq --filter "name=^${PG_CONTAINER}$" 2>/dev/null)" ]]; then
    echo "postgres: $(docker ps -a --filter "name=^${PG_CONTAINER}$" --format '{{.Status}}') (port $PG_PORT, volume $PG_VOLUME)"
  fi
  recorded_pid_files
  if [[ -z "${RECORDED_PID_FILES[@]+x}" ]]; then
    echo "rig: no cluster recorded"
    return 0
  fi

  local f pid idx silo_port gw_port grpc_port http_port cluster_id
  for f in "${RECORDED_PID_FILES[@]}"; do
    if [[ "$f" == "$PIDFILE.partial" ]]; then
      echo "rig: note: a previous 'up' was interrupted mid-spawn; silos below were read from $f"
    fi
    while read -r pid idx silo_port gw_port grpc_port http_port cluster_id; do
      [[ -n "$pid" ]] || continue
      local alive="dead"
      if is_rig_silo_pid "$pid"; then
        alive="alive"
      elif kill -0 "$pid" 2>/dev/null; then
        alive="recycled: pid is alive but not a RigSilo"
      fi
      local health="unreachable"
      curl -sf -o /dev/null "http://127.0.0.1:$http_port/healthz" && health="ok"
      echo "silo $idx: pid=$pid ($alive) grpc=127.0.0.1:$grpc_port http=127.0.0.1:$http_port healthz=$health cluster=$cluster_id"
    done < "$f"
  done
}

# ---- ab ------------------------------------------------------------------------------------

cmd_ab() {
  local n="$DEFAULT_AB_SILOS" trials="$DEFAULT_AB_TRIALS"
  local passthrough=()
  local arg
  for arg in "$@"; do
    case "$arg" in
      --silos=*) n="${arg#--silos=}" ;;
      --trials=*) trials="${arg#--trials=}" ;;
      --co-locate=*) usage_error "ab: --co-locate is driven by the procedure itself (off, then on); remove it." ;;
      *) passthrough+=("$arg") ;;
    esac
  done
  [[ "$n" =~ ^[0-9]+$ && "$n" -ge 1 ]] || usage_error "ab: --silos must be a positive integer."
  [[ "$trials" =~ ^[0-9]+$ && "$trials" -ge 1 ]] || usage_error "ab: --trials must be a positive integer."

  mkdir -p "$RESULTS_DIR"
  CLEANUP_ON_EXIT=1
  local stamp
  stamp="$(date +%Y%m%dT%H%M%S)"

  local colocate t endpoints rig_arg out
  for colocate in off on; do
    for ((t = 1; t <= trials; t++)); do
      echo "== rig ab: co-locate=$colocate trial $t/$trials ==" >&2
      cmd_up "$n" "--co-locate=$colocate"
      endpoints="$(grpc_endpoints "$n")"
      rig_arg="$(rig_hosts "$n")"
      out="$RESULTS_DIR/ab-colocate-${colocate}-trial${t}-${stamp}.json"
      dotnet run -c Release --project "$BENCH_PROJECT" -- remote-check \
        --endpoints="$endpoints" --rig="$rig_arg" --json="$out" ${passthrough[@]+"${passthrough[@]}"}
      echo "rig: wrote $out" >&2
      cmd_down
    done
  done

  CLEANUP_ON_EXIT=0
  echo "rig: A/B results written under $RESULTS_DIR (never inside the repo)"

  if command -v jq >/dev/null 2>&1; then
    echo
    echo "rig: quick delta summary (checks/s, p50 ms, p99 ms):"
    for colocate in off on; do
      for ((t = 1; t <= trials; t++)); do
        out="$RESULTS_DIR/ab-colocate-${colocate}-trial${t}-${stamp}.json"
        [[ -f "$out" ]] || continue
        jq -r '"'"$colocate"' trial '"$t"': checksPerSecond=\(.checksPerSecond) p50ms=\(.check.P50Micros/1000) p99ms=\(.check.P99Micros/1000)"' \
          "$out"
      done
    done
  fi
}

# ---- teardown on interruption ---------------------------------------------------------------

on_interrupt() {
  echo >&2
  echo "rig: interrupted, tearing down whatever this run started..." >&2
  CLEANUP_ON_EXIT=0
  cmd_down || true
  exit 130
}
trap on_interrupt INT TERM

# EXIT-trap cleanup is scoped to `ab` only: a successful `up` leaves the cluster running by
# design, so it must not tear down on normal exit — but an `ab` aborting between its own
# up and down (e.g. a fatal shell error under set -u) must never leave a live cluster.
CLEANUP_ON_EXIT=0
on_exit() {
  if (( CLEANUP_ON_EXIT == 1 )); then
    echo "rig: abnormal exit mid-ab, tearing down whatever this run started..." >&2
    cmd_down || true
  fi
}
trap on_exit EXIT

# ---- main ------------------------------------------------------------------------------------

require_tool dotnet
require_tool curl
require_tool lsof
require_tool pgrep

command="${1:-}"
[[ -n "$command" ]] || { print_usage; exit 2; }
shift || true

case "$command" in
  up) cmd_up "$@" ;;
  down) cmd_down "$@" ;;
  status) cmd_status "$@" ;;
  ab) cmd_ab "$@" ;;
  --help|-h|help) print_usage ;;
  *) usage_error "unknown command '$command'." ;;
esac
