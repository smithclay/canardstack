#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PREFIX="${CANARDSTACK_ORB_PREFIX:-canardstack-bench}"
PROFILES="${CANARDSTACK_ORB_PROFILES:-logs spans}"
DISTRO="${CANARDSTACK_ORB_DISTRO:-ubuntu}"
SOURCE_DIR="$(pwd -P)"
SOURCE_DIR_IN_VM="${CANARDSTACK_ORB_SOURCE_DIR:-/mnt/mac${SOURCE_DIR}}"
CACHE_ROOT="${CANARDSTACK_ORB_CACHE_ROOT:-/var/tmp/canardstack-bench}"
BENCH_PORT="${CANARDSTACK_ORB_PORT:-4318}"
HOST_TARGET_ROOT="${CANARDSTACK_HOST_TARGET_ROOT:-target/canardstack-bench-host}"
HOST_USER="$(id -un)"

usage() {
  cat <<USAGE
Usage: $0 <command> [args...]

Commands:
  up                    Create/start and provision all benchmark server VMs.
  start                 Start all benchmark server VMs.
  stop                  Stop all benchmark server VMs.
  status                Show OrbStack info for all benchmark server VMs.
  shell <profile>       Open a shell in one server VM with benchmark env loaded.
  run <profile> -- CMD  Run a command in one server VM with benchmark env loaded.
  reset-data <profile>  Remove one profile's Canardstack data, storage, and local DuckLake catalog.
  env <profile>         Print the generated benchmark environment file.
  host-commands         Print Mac-side commands for parallel benchmark drivers.
  delete                Delete all benchmark server VMs after confirmation.

Profiles:
  ${PROFILES}

Environment overrides:
  CANARDSTACK_ORB_PREFIX       default: ${PREFIX}
  CANARDSTACK_ORB_PROFILES     default: ${PROFILES}
  CANARDSTACK_ORB_DISTRO       default: ${DISTRO}
  CANARDSTACK_ORB_SOURCE_DIR   default: ${SOURCE_DIR_IN_VM}
  CANARDSTACK_ORB_CACHE_ROOT   default: ${CACHE_ROOT}
  CANARDSTACK_ORB_PORT         default: ${BENCH_PORT}
  CANARDSTACK_HOST_TARGET_ROOT default: ${HOST_TARGET_ROOT}

Each profile maps to one Linux server VM named CANARDSTACK_ORB_PREFIX-profile.
The source tree is shared through OrbStack's macOS file sharing. Linux Cargo
output, Canardstack data, DuckDB files, and temporary files stay inside each VM
under CANARDSTACK_ORB_CACHE_ROOT/<machine>/.
USAGE
}

need_orb() {
  if ! command -v orb >/dev/null 2>&1; then
    echo "error: orb CLI not found. Install and start OrbStack first." >&2
    exit 1
  fi
}

profile_exists() {
  local wanted="$1"
  local profile
  for profile in ${PROFILES}; do
    if [ "${profile}" = "${wanted}" ]; then
      return 0
    fi
  done
  return 1
}

machine_for_profile() {
  local profile="${1:-}"
  if ! profile_exists "${profile}"; then
    echo "error: profile must be one of: ${PROFILES}" >&2
    exit 1
  fi
  printf '%s-%s\n' "${PREFIX}" "${profile}"
}

all_machines() {
  local profile
  for profile in ${PROFILES}; do
    machine_for_profile "${profile}"
  done
}

machine_exists() {
  orb info "$1" >/dev/null 2>&1
}

server_url_for_profile() {
  local profile="$1"
  printf 'http://%s.orb.local:%s\n' "$(machine_for_profile "${profile}")" "${BENCH_PORT}"
}

run_root_setup() {
  local machine="$1"

  orb -m "${machine}" -u root bash -s -- "${HOST_USER}" "${CACHE_ROOT}" <<'REMOTE'
set -euo pipefail

bench_user="$1"
cache_root="$2"
marker="${cache_root}/.apt-provisioned-v1"

if ! command -v apt-get >/dev/null 2>&1; then
  echo "error: this helper currently provisions Debian/Ubuntu-style machines with apt-get" >&2
  exit 1
fi

mkdir -p "${cache_root}"

if [ ! -e "${marker}" ]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libssl-dev \
    pkg-config
  touch "${marker}"
fi

mkdir -p "${cache_root}"
if id "${bench_user}" >/dev/null 2>&1; then
  chown -R "${bench_user}:${bench_user}" "${cache_root}"
fi
REMOTE
}

run_user_setup() {
  local profile="$1"
  local machine
  machine="$(machine_for_profile "${profile}")"

  orb -m "${machine}" bash -s -- \
    "${machine}" \
    "${profile}" \
    "${SOURCE_DIR_IN_VM}" \
    "${CACHE_ROOT}" \
    "${BENCH_PORT}" <<'REMOTE'
set -euo pipefail

machine="$1"
profile="$2"
source_dir="$3"
cache_root="$4"
bench_port="$5"
machine_cache="${cache_root}/${machine}"

if [ ! -d "${source_dir}" ]; then
  echo "error: shared source directory is not visible in Linux: ${source_dir}" >&2
  exit 1
fi

mkdir -p \
  "${machine_cache}/target" \
  "${machine_cache}/data" \
  "${machine_cache}/storage" \
  "${machine_cache}/tmp"

if [ -f "${HOME}/.cargo/env" ]; then
  # shellcheck disable=SC1091
  . "${HOME}/.cargo/env"
fi

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
  # shellcheck disable=SC1091
  . "${HOME}/.cargo/env"
fi

if command -v rustup >/dev/null 2>&1; then
  rustup toolchain install stable --profile minimal
  rustup default stable
fi

cat > "${HOME}/.canardstack-bench-env" <<ENV
export CANARDSTACK_BENCH_MACHINE="${machine}"
export CANARDSTACK_BENCH_PROFILE="${profile}"
export CANARDSTACK_BENCH_BASE_URL="http://${machine}.orb.local:${bench_port}"
export CANARDSTACK_SOURCE_DIR="${source_dir}"
export CANARDSTACK_CACHE_DIR="${machine_cache}"
export CARGO_TARGET_DIR="${machine_cache}/target"
export CANARDSTACK_BIND="0.0.0.0:${bench_port}"
export CANARDSTACK_DATA_DIR="${machine_cache}/data"
export TMPDIR="${machine_cache}/tmp"
export PATH="\${HOME}/.cargo/bin:\${PATH}"
cd "${source_dir}"
ENV

cat > "${HOME}/canardstack-bench" <<'RUNNER'
#!/usr/bin/env bash
set -euo pipefail
# shellcheck disable=SC1091
. "${HOME}/.canardstack-bench-env"
exec "$@"
RUNNER
chmod +x "${HOME}/canardstack-bench"

cargo --version
rustc --version
REMOTE
}

ensure_profile() {
  local profile="$1"
  local machine
  machine="$(machine_for_profile "${profile}")"

  if machine_exists "${machine}"; then
    echo "==> Starting existing OrbStack machine: ${machine}"
    orb start "${machine}" >/dev/null
  else
    echo "==> Creating OrbStack machine: ${machine} (${DISTRO})"
    orb create "${DISTRO}" "${machine}"
  fi

  echo "==> Provisioning benchmark dependencies: ${machine}"
  run_root_setup "${machine}"
  run_user_setup "${profile}"
}

start_machines() {
  local machine
  for machine in $(all_machines); do
    echo "==> Starting OrbStack machine: ${machine}"
    orb start "${machine}" >/dev/null
  done
}

stop_machines() {
  local machine
  for machine in $(all_machines); do
    if machine_exists "${machine}"; then
      echo "==> Stopping OrbStack machine: ${machine}"
      orb stop "${machine}" >/dev/null
    fi
  done
}

status_machines() {
  local machine
  for machine in $(all_machines); do
    echo "==> ${machine}"
    if machine_exists "${machine}"; then
      orb info "${machine}"
    else
      echo "not created"
    fi
  done
}

delete_machines() {
  local machines reply
  machines="$(all_machines | tr '\n' ' ')"
  echo "This will delete OrbStack machines: ${machines}"
  read -r -p "Type 'delete' to continue: " reply
  if [ "${reply}" != "delete" ]; then
    echo "aborted"
    return 0
  fi

  local machine
  for machine in $(all_machines); do
    if machine_exists "${machine}"; then
      echo "==> Deleting OrbStack machine: ${machine}"
      orb delete "${machine}"
    fi
  done
}

open_shell() {
  local machine
  machine="$(machine_for_profile "$1")"
  # shellcheck disable=SC2016
  orb -m "${machine}" bash -lc '. "${HOME}/.canardstack-bench-env"; exec bash -i'
}

run_command() {
  local machine
  machine="$(machine_for_profile "$1")"
  shift
  if [ "${1:-}" = "--" ]; then
    shift
  fi
  if [ "$#" -eq 0 ]; then
    echo "error: missing command" >&2
    exit 1
  fi
  # shellcheck disable=SC2016
  orb -m "${machine}" bash -lc '. "${HOME}/.canardstack-bench-env"; exec "$@"' bash "$@"
}

reset_profile_data() {
  local machine
  machine="$(machine_for_profile "$1")"
  orb -m "${machine}" bash -lc '
    set -euo pipefail
    . "${HOME}/.canardstack-bench-env"
    duckdb_path="${CANARDSTACK_DATA_DIR}/canardstack.duckdb"
    storage_dir="${CANARDSTACK_DATA_DIR}/storage"
    ducklake_catalog="${CANARDSTACK_DATA_DIR}/canardstack.ducklake"
    rm -rf \
      "${CANARDSTACK_DATA_DIR}" \
      "${storage_dir}" \
      "${duckdb_path}" \
      "${ducklake_catalog}" \
      "${TMPDIR}"
    mkdir -p "${CANARDSTACK_DATA_DIR}" "${storage_dir}" "${TMPDIR}"
    printf "reset %s\n" "${CANARDSTACK_CACHE_DIR}"
  '
}

print_env() {
  local machine
  machine="$(machine_for_profile "$1")"
  # shellcheck disable=SC2016
  orb -m "${machine}" bash -lc 'cat "${HOME}/.canardstack-bench-env"'
}

print_host_commands() {
  local profile signals url

  echo "# Start one Canardstack server per VM, usually in separate terminals:"
  for profile in ${PROFILES}; do
    echo "$0 run ${profile} -- cargo run -- serve"
  done

  echo
  echo "# Then run Mac-side benchmark drivers in parallel:"
  for profile in ${PROFILES}; do
    signals="${profile}"
    if [ "${signals}" = "traces" ]; then
      signals="spans"
    fi
    url="$(server_url_for_profile "${profile}")"
    printf 'CARGO_TARGET_DIR=%q cargo bench --bench throughput_iteration -- --base-url %q --signals %q --report-dir %q &\n' \
      "${HOST_TARGET_ROOT}/${profile}" \
      "${url}" \
      "${signals}" \
      "target/canardstack-bench/${profile}"
  done
  echo "wait"
}

main() {
  case "${1:-}" in
    up)
      need_orb
      for profile in ${PROFILES}; do
        ensure_profile "${profile}"
      done
      ;;
    start)
      need_orb
      start_machines
      ;;
    stop)
      need_orb
      stop_machines
      ;;
    status)
      need_orb
      status_machines
      ;;
    shell)
      need_orb
      shift
      open_shell "${1:-}"
      ;;
    run)
      need_orb
      shift
      run_command "$@"
      ;;
    reset-data)
      need_orb
      shift
      reset_profile_data "${1:-}"
      ;;
    env)
      need_orb
      shift
      print_env "${1:-}"
      ;;
    host-commands)
      print_host_commands
      ;;
    delete)
      need_orb
      delete_machines
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
