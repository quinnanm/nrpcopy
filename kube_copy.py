#!/usr/bin/env python3
"""
kube_copy.py — Copy .root files from UAF to an NRP Kubernetes PVC via krsync.

Runs ON UAF. Splits files into batches, launches each batch as a background
rsync process through krsync into a long-lived NRP pod with the PVC mounted.

Modes:
  Normal (default) — dispatch batches up to --max-parallel at a time,
                     block and show live progress, print summary at end.

  --no-wait        — fire all batches (up to --max-parallel at a time) as
                     nohup background processes and return immediately.
                     Use --summarize later to check results.

  --summarize LOGS — parse completed batch log files and print the summary
                     + resubmit command. No copying is done.
                     e.g.  python kube_copy.py --summarize copy_logs/batch_0520-*.log

Usage:
    python kube_copy.py \
        --input-dirs /ceph/cms/store/user/mequinna/ntuples/QCD \
                     /ceph/cms/store/user/mequinna/ntuples/TTbar \
        --output-path /data/ntuples \
        --namespace axol1tl \
        --pvc mequinna-pvc \
        --prefix QCD TTbar \
        --files-per-job 100 \
        --max-parallel 4 \
        --flat \
        --no-wait

    # later, once logs are done:
    python kube_copy.py --summarize copy_logs/batch_0520-142301_*.log
"""

import argparse
import subprocess
import sys
import time
import json
import os
import shlex
import tempfile
from pathlib import Path
from datetime import datetime

# ── ANSI colors ───────────────────────────────────────────────────────────────
G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; B = "\033[94m"; E = "\033[0m"

def log(msg, color=""):  print(f"{color}[{datetime.now():%H:%M:%S}] {msg}{E}", flush=True)
def info(msg): log(msg, B)
def ok(msg):   log(msg, G)
def warn(msg): log(msg, Y)
def err(msg):  log(msg, R)
def div():     print(Y + "─" * 60 + E, flush=True)


# ── kubectl helpers ───────────────────────────────────────────────────────────

def kubectl(args, namespace, capture=False, check=True):
    cmd = ["kubectl", "-n", namespace] + args
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True, check=check)
    return subprocess.run(cmd, check=check)


def pod_phase(pod, namespace):
    r = kubectl(["get", "pod", pod, "-o", "jsonpath={.status.phase}"],
                namespace, capture=True, check=False)
    return r.stdout.strip() if r.returncode == 0 else None


def pod_exists(pod, namespace):
    return pod_phase(pod, namespace) is not None


def wait_for_pod(pod, namespace, timeout=180):
    info(f"Waiting for pod {pod} to be Running …")
    for _ in range(timeout // 5):
        phase = pod_phase(pod, namespace)
        if phase == "Running":
            ok(f"  Pod {pod} is Running.")
            return True
        if phase in ("Failed", "Unknown"):
            err(f"  Pod {pod} entered phase: {phase}")
            return False
        time.sleep(5)
    err(f"  Timed out waiting for {pod}.")
    return False


def create_copy_pod(pod, namespace, pvc):
    manifest = {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": pod, "namespace": namespace},
        "spec": {
            "containers": [{
                "name": "copy", "image": "ubuntu:22.04",
                "command": ["sleep", "infinity"],
                "volumeMounts": [{"mountPath": "/data", "name": "pvc"}],
            }],
            "volumes": [{"name": "pvc", "persistentVolumeClaim": {"claimName": pvc}}],
        },
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(manifest, f)
        tmp = f.name
    try:
        subprocess.run(["kubectl", "-n", namespace, "apply", "-f", tmp], check=True)
    finally:
        os.unlink(tmp)


def exec_in_pod(pod, namespace, cmd_args):
    return kubectl(["exec", pod, "--"] + cmd_args,
                   namespace, capture=True, check=False)


def file_exists_in_pod(pod, namespace, path):
    return exec_in_pod(pod, namespace, ["test", "-f", path]).returncode == 0


def mkdir_in_pod(pod, namespace, path):
    exec_in_pod(pod, namespace, ["mkdir", "-p", path])


# ── krsync wrapper ────────────────────────────────────────────────────────────

KRSYNC_SCRIPT = """\
#!/bin/bash
if [ -z "$KRSYNC_STARTED" ]; then
    export KRSYNC_STARTED=true
    exec rsync --blocking-io --rsh "$0" $@
fi
namespace=''
pod=$1
shift
if [ "X$pod" = "X-l" ]; then
    pod=$1; shift; namespace="-n $1"; shift
fi
exec kubectl $namespace exec -i $pod -- "$@"
"""

def ensure_krsync(path):
    p = Path(path)
    if not p.exists():
        p.write_text(KRSYNC_SCRIPT)
        p.chmod(0o755)
        ok(f"Wrote krsync wrapper to {path}")
    return str(p.resolve())


# ── File discovery ────────────────────────────────────────────────────────────

def find_root_files(directory):
    d = Path(directory)
    if not d.exists():
        warn(f"Directory not found: {directory}")
        return []
    return sorted(d.rglob("*.root"))


def build_output_path(src_file, src_base, output_base, prefix, flat):
    fname = src_file.name
    if prefix:
        fname = f"{prefix}_{fname}"
    if flat:
        return f"{output_base}/{fname}"
    rel_parent = str(src_file.relative_to(src_base).parent)
    if rel_parent == ".":
        return f"{output_base}/{fname}"
    return f"{output_base}/{rel_parent}/{fname}"


# ── Batch shell script builder ────────────────────────────────────────────────

def build_batch_script(batch, pod, namespace, krsync_path):
    """
    Write a shell script that rsyncs each file in the batch via krsync,
    then prints DONE or FAILED lines for each file.
    Returns the script content as a string.
    """
    lines = ["#!/bin/bash", "set -uo pipefail", ""]
    for src, dst in batch:
        dst_dir = shlex.quote(str(Path(dst).parent))
        src_q   = shlex.quote(str(src))
        dst_q   = shlex.quote(dst)
        pod_dst = f"{pod}:{dst}"
        lines += [
            f"# {src.name}",
            f"kubectl -n {namespace} exec {pod} -- mkdir -p {dst_dir}",
            f"if {shlex.quote(krsync_path)} -av --progress --stats {src_q} {shlex.quote(pod_dst)}; then",
            # Verify size after copy
            f"  local_size=$(stat -c '%s' {src_q})",
            f"  remote_size=$(kubectl -n {namespace} exec {pod} -- stat -c '%s' {dst_q} 2>/dev/null || echo 0)",
            f"  if [ \"$local_size\" = \"$remote_size\" ]; then",
            f"    echo \"OK: {src}\"",
            f"  else",
            f"    echo \"SIZEMISMATCH: {src} (local=$local_size remote=$remote_size)\"",
            f"  fi",
            f"else",
            f"  echo \"FAILED: {src}\"",
            f"fi",
            "",
        ]
    lines.append("echo BATCH_DONE")
    return "\n".join(lines)


# ── Batch runner ──────────────────────────────────────────────────────────────

class BatchProcess:
    def __init__(self, idx, batch, script_path, log_path, dry_run):
        self.idx         = idx
        self.batch       = batch          # list of (src, dst)
        self.script_path = script_path
        self.log_path    = log_path
        self.dry_run     = dry_run
        self.proc        = None
        self.started_at  = None

    def start(self):
        if self.dry_run:
            info(f"  [DRY RUN] Would launch batch {self.idx:03d} "
                 f"({len(self.batch)} files) → {self.log_path}")
            return
        self.started_at = datetime.now()
        log_f = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            ["bash", self.script_path],
            stdout=log_f, stderr=log_f,
            start_new_session=True,   # survives terminal disconnect
        )
        info(f"  Launched batch {self.idx:03d} "
             f"({len(self.batch)} files) PID={self.proc.pid} → {self.log_path}")

    def poll(self):
        """Return True if finished (or dry run)."""
        if self.dry_run:
            return True
        if self.proc is None:
            return True
        return self.proc.poll() is not None

    def returncode(self):
        if self.dry_run or self.proc is None:
            return 0
        return self.proc.returncode

    def elapsed(self):
        if self.started_at is None:
            return 0
        return (datetime.now() - self.started_at).seconds

    def parse_results(self):
        """Read log and return (ok_srcs, failed_srcs, size_mismatch_srcs)."""
        if self.dry_run:
            return [src for src, _ in self.batch], [], []
        ok_srcs, failed_srcs, mismatch_srcs = [], [], []
        try:
            with open(self.log_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("OK: "):
                        ok_srcs.append(line[4:])
                    elif line.startswith("FAILED: "):
                        failed_srcs.append(line[8:])
                    elif line.startswith("SIZEMISMATCH: "):
                        mismatch_srcs.append(line[14:])
        except FileNotFoundError:
            failed_srcs = [str(src) for src, _ in self.batch]
        return ok_srcs, failed_srcs, mismatch_srcs


# ── Resubmit command ──────────────────────────────────────────────────────────

def build_resubmit_command(failed_srcs, args):
    parts = [
        "python kube_copy.py",
        f"  --input-dirs {' '.join(shlex.quote(s) for s in failed_srcs)}",
        f"  --output-path {shlex.quote(args.output_path)}",
        f"  --namespace {shlex.quote(args.namespace)}",
        f"  --pvc {shlex.quote(args.pvc)}",
        f"  --copy-pod {shlex.quote(args.copy_pod)}",
        f"  --files-per-job {args.files_per_job}",
        f"  --max-parallel {args.max_parallel}",
        f"  --krsync {shlex.quote(args.krsync)}",
        "  --skip-existing",
    ]
    if args.flat:
        parts.append("  --flat")
    return " \\\n".join(parts)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Copy .root files from UAF to NRP Kubernetes PVC via krsync.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input-dirs", nargs="+", required=True,
        metavar="DIR", help="Source directories on UAF")
    p.add_argument("--output-path", required=True,
        metavar="PATH", help="Destination path inside the PVC (e.g. /data/ntuples)")
    p.add_argument("--namespace", "-n", default="axol1tl",
        help="Kubernetes namespace (default: axol1tl)")
    p.add_argument("--pvc", required=True,
        metavar="PVC_NAME", help="PVC name to mount (e.g. mequinna-pvc)")
    p.add_argument("--copy-pod", default="copy-pod",
        help="Name of long-lived pod with PVC mounted (default: copy-pod)")
    p.add_argument("--create-pod", action="store_true",
        help="Create the copy pod if it doesn't exist")
    p.add_argument("--prefix", nargs="*", default=None,
        metavar="STR",
        help="Prefix per input dir — e.g. --prefix QCD TTbar → QCD_file.root")
    p.add_argument("--flat", action="store_true",
        help="Put all files flat in output-path (no subdir structure)")
    p.add_argument("--files-per-job", type=int, default=100,
        metavar="N", help="Files per batch (default: 100)")
    p.add_argument("--max-parallel", type=int, default=4,
        metavar="N", help="Max batches running simultaneously (default: 4)")
    p.add_argument("--krsync", default="./krsync",
        help="Path to krsync wrapper script (created if missing)")
    p.add_argument("--skip-existing", action="store_true",
        help="Skip files already present in the pod")
    p.add_argument("--log-dir", default="./copy_logs",
        help="Directory for per-batch log files (default: ./copy_logs)")
    p.add_argument("--log-file", default="copy_summary.json",
        help="JSON summary log (default: copy_summary.json)")
    p.add_argument("--no-wait", action="store_true",
        help="Fire all batches in the background and return immediately. "
             "Use --summarize later to check results.")
    p.add_argument("--summarize", nargs="+", metavar="LOG",
        help="Parse completed batch log files and print summary + resubmit command. "
             "No copying is done. Accepts globs: copy_logs/batch_0520-*.log")
    p.add_argument("--dry-run", action="store_true",
        help="Print actions without executing anything")
    return p.parse_args()


# ── Summarize mode ────────────────────────────────────────────────────────────

def summarize_logs(log_paths, args):
    """Parse a list of completed batch log files and print summary."""
    import glob as _glob

    # Expand any globs (argparse doesn't do this on Linux)
    expanded = []
    for p in log_paths:
        matches = _glob.glob(p)
        if matches:
            expanded.extend(sorted(matches))
        else:
            expanded.append(p)  # let the open() fail naturally

    if not expanded:
        err("No log files found.")
        sys.exit(1)

    all_ok, all_failed, all_mismatch, still_running = [], [], [], []

    for log_path in expanded:
        try:
            with open(log_path) as f:
                lines = f.readlines()
        except FileNotFoundError:
            warn(f"Log not found: {log_path}")
            continue

        batch_done = any("BATCH_DONE" in l for l in lines)
        if not batch_done:
            still_running.append(log_path)
            continue

        for line in lines:
            line = line.strip()
            if line.startswith("OK: "):
                all_ok.append(line[4:])
            elif line.startswith("FAILED: "):
                all_failed.append(line[8:])
            elif line.startswith("SIZEMISMATCH: "):
                all_mismatch.append(line[14:])

    div()
    if still_running:
        warn(f"Still running ({len(still_running)} batches — no BATCH_DONE yet):")
        for p in still_running:
            warn(f"  {p}")
        print()

    ok(f"✓ {len(all_ok)} succeeded  "
       f"✗ {len(all_failed)} failed  "
       f"⚠ {len(all_mismatch)} size mismatches  "
       f"⏳ {len(still_running)} still running")

    problem_srcs = all_failed + [s.split(" ")[0] for s in all_mismatch]

    if all_failed:
        print()
        err("══ FAILED FILES ══════════════════════════════════════")
        for s in all_failed:
            err(f"  {s}")

    if all_mismatch:
        print()
        warn("══ SIZE MISMATCHES ═══════════════════════════════════")
        for s in all_mismatch:
            warn(f"  {s}")

    if problem_srcs and hasattr(args, "output_path") and args.output_path:
        print()
        warn("══ RESUBMIT COMMAND ══════════════════════════════════")
        print(Y + build_resubmit_command(problem_srcs, args) + E)

    div()


def main():
    args = parse_args()

    # ── Summarize-only mode ───────────────────────────────────────────────────
    if args.summarize:
        summarize_logs(args.summarize, args)
        sys.exit(0)

    # Validate prefixes
    prefixes = args.prefix or []
    if prefixes and len(prefixes) != len(args.input_dirs):
        err(f"--prefix count ({len(prefixes)}) must match --input-dirs "
            f"({len(args.input_dirs)})")
        sys.exit(1)
    if not prefixes:
        prefixes = [None] * len(args.input_dirs)

    krsync = ensure_krsync(args.krsync)
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # ── Pod setup ────────────────────────────────────────────────────────────
    if args.create_pod and not pod_exists(args.copy_pod, args.namespace):
        info(f"Creating pod {args.copy_pod} …")
        create_copy_pod(args.copy_pod, args.namespace, args.pvc)

    if not args.dry_run:
        if not wait_for_pod(args.copy_pod, args.namespace):
            sys.exit(1)

    # ── Collect files ─────────────────────────────────────────────────────────
    all_jobs = []
    for src_dir, prefix in zip(args.input_dirs, prefixes):
        files = find_root_files(src_dir)
        info(f"Found {len(files)} .root files in {src_dir}")
        for f in files:
            dst = build_output_path(
                f, Path(src_dir), args.output_path, prefix, args.flat)
            all_jobs.append((f, dst))

    if not all_jobs:
        warn("No .root files found. Exiting.")
        sys.exit(0)

    # ── Skip existing ─────────────────────────────────────────────────────────
    if args.skip_existing and not args.dry_run:
        info("Checking for existing files in pod …")
        before = len(all_jobs)
        all_jobs = [
            (src, dst) for src, dst in all_jobs
            if not file_exists_in_pod(args.copy_pod, args.namespace, dst)
        ]
        skipped = before - len(all_jobs)
        if skipped:
            warn(f"  Skipping {skipped} already-present files.")

    if not all_jobs:
        ok("All files already present. Nothing to do.")
        sys.exit(0)

    # ── Split into batches ────────────────────────────────────────────────────
    n = args.files_per_job
    batches = [all_jobs[i:i+n] for i in range(0, len(all_jobs), n)]
    ts = datetime.now().strftime("%m%d-%H%M%S")

    div()
    info(f"Files to copy : {len(all_jobs)}")
    info(f"Batch size    : {args.files_per_job}")
    info(f"Total batches : {len(batches)}")
    info(f"Max parallel  : {args.max_parallel}")
    info(f"Pod           : {args.copy_pod}  namespace={args.namespace}")
    info(f"PVC           : {args.pvc}")
    div()

    # Write batch scripts
    batch_procs = []
    for idx, batch in enumerate(batches):
        script_path = log_dir / f"batch_{ts}_{idx:03d}.sh"
        log_path    = log_dir / f"batch_{ts}_{idx:03d}.log"
        script_content = build_batch_script(
            batch, args.copy_pod, args.namespace, krsync)
        script_path.write_text(script_content)
        script_path.chmod(0o755)
        batch_procs.append(
            BatchProcess(idx, batch, str(script_path), str(log_path), args.dry_run)
        )

    # ── Dispatch loop ─────────────────────────────────────────────────────────
    queue   = list(batch_procs)
    running = []
    finished = []

    if args.no_wait:
        # Fire up to max_parallel batches and return immediately.
        # Remaining batches are NOT auto-started — user can re-run or
        # increase --max-parallel. This keeps things simple and safe.
        info("Starting batches (--no-wait mode) …\n")
        while queue and len(running) < args.max_parallel:
            bp = queue.pop(0)
            bp.start()
            running.append(bp)
        print()
        ok(f"Launched {len(running)} batch(es) in the background.")
        if queue:
            warn(f"{len(queue)} batch(es) not started (increase --max-parallel to run more at once).")
        info("Check progress with:")
        info(f"  tail -f {args.log_dir}/batch_{ts}_*.log")
        info("When done, get summary with:")
        info(f"  python kube_copy.py --summarize {args.log_dir}/batch_{ts}_*.log "
             f"--output-path {shlex.quote(args.output_path)} "
             f"--namespace {shlex.quote(args.namespace)} "
             f"--pvc {shlex.quote(args.pvc)} "
             f"--copy-pod {shlex.quote(args.copy_pod)}")
        sys.exit(0)

    # Normal blocking mode
    info("Starting batch dispatch …\n")

    while queue or running:
        # Fill up to max_parallel
        while queue and len(running) < args.max_parallel:
            bp = queue.pop(0)
            bp.start()
            running.append(bp)

        # Check for finished
        still_running = []
        for bp in running:
            if bp.poll():
                finished.append(bp)
                rc = bp.returncode()
                elapsed = bp.elapsed()
                if rc == 0:
                    ok(f"  Batch {bp.idx:03d} finished in {elapsed}s ✓")
                else:
                    err(f"  Batch {bp.idx:03d} finished with rc={rc} in {elapsed}s ✗")
            else:
                still_running.append(bp)
        running = still_running

        if running:
            done_count = len(finished)
            total = len(batch_procs)
            running_ids = ", ".join(f"{b.idx:03d}" for b in running)
            print(f"\r  [{done_count}/{total} done] running: [{running_ids}]   ",
                  end="", flush=True)
            time.sleep(3)

    print()  # newline after progress line

    # ── Collect results ───────────────────────────────────────────────────────
    all_ok       = []
    all_failed   = []
    all_mismatch = []

    for bp in finished:
        ok_s, fail_s, mm_s = bp.parse_results()
        all_ok.extend(ok_s)
        all_failed.extend(fail_s)
        all_mismatch.extend(mm_s)

    # ── Summary ───────────────────────────────────────────────────────────────
    div()
    ok(f"Done.  ✓ {len(all_ok)} succeeded  "
       f"✗ {len(all_failed)} failed  "
       f"⚠ {len(all_mismatch)} size mismatches")

    problem_srcs = all_failed + all_mismatch

    if all_failed:
        print()
        err("══ FAILED FILES ══════════════════════════════════════")
        for s in all_failed:
            err(f"  {s}")

    if all_mismatch:
        print()
        warn("══ SIZE MISMATCHES ═══════════════════════════════════")
        for s in all_mismatch:
            warn(f"  {s}")

    if problem_srcs:
        print()
        warn("══ RESUBMIT COMMAND ══════════════════════════════════")
        print(Y + build_resubmit_command(problem_srcs, args) + E)
        print()

    info(f"Batch logs in: {args.log_dir}/batch_{ts}_*.log")

    # ── JSON summary ──────────────────────────────────────────────────────────
    file_results = []
    for bp in finished:
        ok_s, fail_s, mm_s = bp.parse_results()
        for src, dst in bp.batch:
            s = str(src)
            if s in ok_s:       status = "ok"
            elif s in fail_s:   status = "failed"
            elif s in mm_s:     status = "size_mismatch"
            else:               status = "unknown"
            file_results.append({"src": s, "dst": dst,
                                  "batch": bp.idx, "status": status})

    with open(args.log_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "namespace": args.namespace,
            "pvc": args.pvc,
            "pod": args.copy_pod,
            "output_path": args.output_path,
            "summary": {
                "total": len(all_jobs),
                "ok": len(all_ok),
                "failed": len(all_failed),
                "size_mismatch": len(all_mismatch),
            },
            "files": file_results,
        }, f, indent=2)
    info(f"Summary log  : {args.log_file}")

    if problem_srcs:
        sys.exit(1)


if __name__ == "__main__":
    main()
PYEOF