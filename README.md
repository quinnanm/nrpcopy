# kube_copy.py

Copy `.root` files from the UAF cluster (`uaf-4.t2.ucsd.edu`) to a PVC on the NRP Nautilus cluster.

The script runs **on UAF**. It uses `krsync` — a thin wrapper that tunnels `rsync` over `kubectl exec` — to stream files directly into a long-lived pod that has your PVC mounted. Files are split into batches and run in parallel background processes.

---

## Prerequisites

All of these need to be set up on UAF before running the script.

**1. kubectl and kubelogin**
Follow the [NRP getting started guide](https://nrp.ai/documentation/userdocs/start/getting-started/). In short:
- Download the `x86_64` kubectl binary, place it in `~/.local/bin/`, add that to your `$PATH`
- Install the `oidc-login` plugin via `kubectl krew install oidc-login`
- Download your kubeconfig from `https://nrp.ai/config`, save as `~/.kube/config`
- Authenticate: `kubectl get pods -n axol1tl`

**2. krsync**
The script will auto-create a `krsync` wrapper in the current directory if one is not found. You can also place an existing one there manually. There should already be a copy in `/ceph/cms/store/user/mequinna/ntuples/`.

**3. A PVC on NRP**
You need a PVC created in your namespace before running. Example:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mequinna-pvc
spec:
  storageClassName: rook-cephfs
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 5000Gi
```
Apply it with:
```bash
kubectl apply -f mequinna-pvc.yaml -n axol1tl
kubectl get pvc -n axol1tl   # wait for STATUS=Bound
```

**4. A copy pod**
A long-lived pod with your PVC mounted needs to be running on NRP. Pass `--create-pod` on your first run and the script will create one automatically. Or create it manually:
```bash
kubectl run copy-pod -n axol1tl \
  --image=ubuntu:22.04 \
  --overrides='{"spec":{"volumes":[{"name":"pvc","persistentVolumeClaim":{"claimName":"mequinna-pvc"}}],"containers":[{"name":"copy-pod","image":"ubuntu:22.04","command":["sleep","infinity"],"volumeMounts":[{"mountPath":"/data","name":"pvc"}]}]}}' \
  -- sleep infinity
```

---

## Basic usage

```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/mequinna/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc mequinna-pvc
```

This will find all `.root` files under the input directory, split them into batches of 100, run up to 4 batches in parallel, block until everything is done, and print a summary.

Always do a **dry run first** to verify the file list and destination paths before copying anything:
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/mequinna/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc mequinna-pvc \
  --dry-run
```

---

## Common recipes

**Copy multiple sample directories with prefixes, flat output:**
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/mequinna/ntuples/QCD \
               /ceph/cms/store/user/mequinna/ntuples/TTbar \
               /ceph/cms/store/user/mequinna/ntuples/WJets \
  --prefix QCD TTbar WJets \
  --flat \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc mequinna-pvc
```

With `--prefix`, each file is renamed `PREFIX_originalname.root`. With `--flat`, all files land in one directory regardless of the subdirectory structure on UAF. Without `--flat`, the subdirectory structure is preserved under `--output-path`.

**First-time run — auto-create the pod:**
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/mequinna/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc mequinna-pvc \
  --create-pod
```

**Fire and forget — return immediately, check later:**
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/mequinna/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc mequinna-pvc \
  --no-wait
```

The script launches batches in the background and exits. The background processes survive SSH disconnection. The script prints the exact `--summarize` command to run when you come back to check results.

**Resume after interruption — skip already-copied files:**
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/mequinna/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc mequinna-pvc \
  --skip-existing
```

---

## Checking results

**While batches are running** (tail all batch logs):
```bash
tail -f copy_logs/batch_0520-142301_*.log
```

**Check which batches are still going:**
```bash
grep -l "BATCH_DONE" copy_logs/batch_0520-142301_*.log   # finished
ps aux | grep batch_                                       # still running
```

**Get the full summary** (works mid-run too — shows which batches aren't done yet):
```bash
python kube_copy.py \
  --summarize copy_logs/batch_0520-142301_*.log \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc mequinna-pvc \
  --copy-pod copy-pod
```

This prints counts of succeeded / failed / size-mismatched files, lists any problem files, and if there are failures prints a ready-to-run resubmit command.

**Resubmitting failures** — just copy-paste the resubmit command from the summary output. It pre-fills `--skip-existing` so already-copied files are not re-copied.

---

## All options

| Flag | Default | Description |
|---|---|---|
| `--input-dirs` | required | One or more source directories on UAF. Recursively finds all `.root` files. |
| `--output-path` | required | Destination path inside the PVC, e.g. `/data/ntuples`. |
| `--namespace` | `axol1tl` | Kubernetes namespace. |
| `--pvc` | required | PVC name, e.g. `mequinna-pvc`. |
| `--copy-pod` | `copy-pod` | Name of the long-lived pod with the PVC mounted. |
| `--create-pod` | off | Create the copy pod if it doesn't exist. |
| `--prefix` | none | One prefix string per input dir. `--prefix QCD TTbar` renames files to `QCD_file.root`, `TTbar_file.root`. Count must match `--input-dirs`. |
| `--flat` | off | Put all output files in one flat directory. Without this, subdirectory structure from the source is preserved. |
| `--files-per-job` | `100` | Number of files per batch. |
| `--max-parallel` | `4` | Maximum number of batches running simultaneously. |
| `--skip-existing` | off | Check the pod before copying and skip files already present. |
| `--no-wait` | off | Launch batches and return immediately. Use `--summarize` to check results later. |
| `--summarize` | off | Parse log files and print summary. No copying. Pass log glob: `--summarize copy_logs/batch_*.log`. Also needs `--output-path`, `--namespace`, `--pvc`, `--copy-pod` for the resubmit command. |
| `--krsync` | `./krsync` | Path to krsync wrapper. Created automatically if missing. |
| `--log-dir` | `./copy_logs` | Directory for per-batch shell scripts and log files. |
| `--log-file` | `copy_summary.json` | JSON file summarising all file statuses at the end of a blocking run. |
| `--dry-run` | off | Print everything that would happen without copying anything. |

---

## How it works

1. The script discovers all `.root` files recursively under each `--input-dirs` path.
2. Files are split into batches of `--files-per-job`. For each batch a shell script is written to `--log-dir`.
3. Up to `--max-parallel` batch scripts run simultaneously as background processes. Each script rsyncs its files via `krsync` (which tunnels rsync over `kubectl exec`) into the copy pod, then checks file sizes to verify each transfer.
4. Each file in the batch log gets a status line: `OK:`, `FAILED:`, or `SIZEMISMATCH:`. The batch ends with `BATCH_DONE`.
5. In blocking mode the script watches all batches and prints a live progress line. In `--no-wait` mode it exits immediately after launch.
6. At the end (or when you run `--summarize`) the logs are parsed and results reported.

---

## Troubleshooting

**Pod not found / not Running**
```bash
kubectl get pods -n axol1tl
kubectl describe pod copy-pod -n axol1tl
```
If the pod is stuck in `Pending`, check PVC status: `kubectl get pvc -n axol1tl`.

**kubectl auth expired**
NRP uses OIDC tokens that expire. Re-authenticate with:
```bash
kubectl get pods -n axol1tl   # triggers browser login
```

**rsync fails immediately**
Make sure the `krsync` file is executable: `chmod +x ./krsync`. Also confirm the copy pod is Running before starting.

**Size mismatch on a file**
The file was partially transferred. Run `--summarize` to get the resubmit command — it will list the affected files and retry them with `--skip-existing` so everything else is left alone.

**Check what's on the PVC**
```bash
kubectl exec -n axol1tl copy-pod -- find /data -name "*.root" | wc -l
kubectl exec -n axol1tl copy-pod -- du -sh /data
```
