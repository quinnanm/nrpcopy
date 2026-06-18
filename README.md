# NRPCOPY

This is a collection of scripts for copying files from the a t2 cluster (ie `uaf-4.t2.ucsd.edu`) to a PVC on the NRP Nautilus cluster. In principle this could be used for copying between any remote cluster and NRP (lxplus, lpc, etc...)

The script runs **on UAF/T2**. It uses `krsync` — a thin wrapper that tunnels `rsync` over `kubectl exec` — to stream files directly into a long-lived pod that has your PVC on the namespace mounted. Files are split into batches and run in parallel background processes. This was designed for the axol1tl namespace, UAF, and the traindatavol pvc but can be generalized. I hope you find it useful! :)

---

## Setup

All of these need to be set up on UAF before things will work

**NRP, kubectl and kubelogin setup**
This comes from the [NRP getting started guide](https://nrp.ai/documentation/userdocs/start/getting-started/). 

0) ssh into your T2 cluster (of course you need access first)

```
#can do uaf-1,2,3 or 4
ssh username@uaf-2.t2.ucsd.edu
```

1) log into [nrp.ai](nrp.ai) in your browser. You need to be in the namespace where the files are being copied.

2) install kubectl

```
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256"
echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
chmod +x kubectl
mkdir -p ~/.local/bin
mv ./kubectl ~/.local/bin/kubectl
```

get the path variables set properly and verify it works:

```
export PATH="$HOME/.local/bin:$PATH"
source ~/.bashrc
which kubectl
kubectl version --client
```

3) install krew +kubelogin (this takes a while)

```
(
  set -x; cd "$(mktemp -d)" &&
  OS="$(uname | tr '[:upper:]' '[:lower:]')" &&
  ARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/arm.*$/arm/')" &&
  KREW="krew-${OS}_${ARCH}" &&
  curl -fsSLO "https://github.com/kubernetes-sigs/krew/releases/latest/download/${KREW}.tar.gz" &&
  tar zxvf "${KREW}.tar.gz" &&
  ./"${KREW}" install krew
)
```
set path, install and verify

```
export PATH="${KREW_ROOT:-$HOME/.krew}/bin:$PATH"
source ~/.bashrc
kubectl krew install oidc-login
kubectl oidc-login --version
```

4) get nrp config file and copy it to the cluster

on the cluster:

```
mkdir ~/.kube
```

on your local machine:
download-> [https://nrp.ai/config](https://nrp.ai/config)

```
scp ~/Downloads/config username@uaf-4.t2.ucsd.edu:~/.kube/config
```

5) log into nrp from t2 cluster (can use namespace of choice, example here is axol1tl)

```
kubectl get nodes
kubectl get pods -n axol1tl

#if you want a default namespace
kubectl config set contexts.nautilus.namespace axol1tl
```

6) add to bashrc

do this so you dont have to src the paths again in another session

```
nano ~/.bashrc
#add these to the bottom:
export PATH="$HOME/.local/bin:$PATH"
export PATH="${KREW_ROOT:-$HOME/.krew}/bin:$PATH"
source ~/.bashrc
```

**Script setup**

0) after doing above, ssh into t2 cluster and clone the git repo

```
bash
git clone https://github.com/quinnanm/nrpcopy.git
cd nrpcopy
```

you should have a few scripts of note: kube_copy.py, krsync and ymls/copy-pod.yml

1) set up the pod for copying

```
kubectl apply -f ymls/copy-pod.yml
#check its running:
kubectl get pods -n axol1tl
```
copy-pod needs to be "Running" for any of this to work.

2) give krsync permissions

this is needed for it to work. if you get permission issues this is a likely culprit

```
chmod +x krsync
ls -la krsync
# should show -rwxr-xr-x
```

3) prepare for liftoff

you need an input directory on the T2 of files you want to copy, a pvc on NRP to copy to, an output directory on that pvc, a namespace, and your running copy-pod

---

## Basic usage

```bash
python kube_copy.py \
  --input-dirs /indir/name \
  --output-path /outdir/name \
  --namespace nameofnamespace \
  --pvc pvc name
```

This will find all `.root` files under the input directory, split them into batches of 100, run up to 4 batches in parallel, block until everything is done, and print a summary.

Always do a **dry run first** to verify the file list and destination paths before copying anything:

```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc username-pvc \
  --dry-run
```

By default the script copies all `.root` files. To copy a different file type:

```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/MyData \
  --output-path /data/ADsamples/MyData \
  --namespace axol1tl \
  --pvc traindatavol \
  --filetype '*.h5'
```

Here is what I did in my working example: flat means there are no nested dirs like the input dirs, just the target output dirs. 

```
#try it
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/VBFHto2B_25 \
  --output-path /data/ADsamples/VBFHto2B_25 \
  --namespace axol1tl \
  --pvc traindatavol \
  --copy-pod copy-pod \
  --files-per-job 50 \
  --max-parallel 4 \
  --flat \
  --dry-run

#submit for real
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/VBFHto2B_25 \
  --output-path /data/ADsamples/VBFHto2B_25 \
  --namespace axol1tl \
  --pvc traindatavol \
  --copy-pod copy-pod \
  --files-per-job 50 \
  --max-parallel 4 \
  --flat

#resubmit failed jobs
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/VBFHto2B_25 \
  --output-path /data/ADsamples/VBFHto2B_25 \
  --namespace axol1tl \
  --pvc traindatavol \
  --copy-pod copy-pod \
  --files-per-job 50 \
  --max-parallel 4 \
  --flat \
  --skip-existing

#or to avoid printouts/holding the command line hostage
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/VBFHto2B_25 \
  --output-path /data/ADsamples/VBFHto2B_25 \
  --namespace axol1tl \
  --pvc traindatavol \
  --copy-pod copy-pod \
  --files-per-job 50 \
  --max-parallel 4 \
  --flat \
  --no-wait
```

log files are printed in copy_logs/.

then check the status on nrp:

```
kubectl exec -it copy-pod -n axol1tl -- bash
ls /data/ADsamples/VBFHto2B_25/
ls /data/ADsamples/VBFHto2B_25/ | wc -l
exit
```

dont forget to delete the pod when done!

```
kubectl delete pod copy-pod -n axol1tl
```

---

## Common recipes

**Copy multiple sample directories with prefixes, flat output:**
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/QCD \
               /ceph/cms/store/user/username/ntuples/TTbar \
               /ceph/cms/store/user/username/ntuples/WJets \
  --prefix QCD TTbar WJets \
  --flat \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc username-pvc
```

With `--prefix`, each file is renamed `PREFIX_originalname.root`. With `--flat`, all files land in one directory regardless of the subdirectory structure on UAF. Without `--flat`, the subdirectory structure is preserved under `--output-path`.

**First-time run — auto-create the pod:**
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc username-pvc \
  --create-pod
```

**Fire and forget — return immediately, check later:**
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc username-pvc \
  --no-wait
```

The script launches batches in the background and exits. The background processes survive SSH disconnection. The script prints the exact `--summarize` command to run when you come back to check results.

**Resume after interruption — skip already-copied files:**
```bash
python kube_copy.py \
  --input-dirs /ceph/cms/store/user/username/ntuples/QCD \
  --output-path /data/ntuples \
  --namespace axol1tl \
  --pvc username-pvc \
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
  --pvc username-pvc \
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
| `--pvc` | required | PVC name, e.g. `username-pvc`. |
| `--copy-pod` | `copy-pod` | Name of the long-lived pod with the PVC mounted. |
| `--create-pod` | off | Create the copy pod if it doesn't exist. |
| `--prefix` | none | One prefix string per input dir. `--prefix QCD TTbar` renames files to `QCD_file.root`, `TTbar_file.root`. Count must match `--input-dirs`. |
| `--filetype` | `*.root` | File pattern to match. e.g. `--filetype '*.h5'` or `--filetype '*'` for all files. |
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

## Reverse copy
 
A second python script, `kube_reversecopy.py` is provided for convenience to copy files FROM the NRP nautilus cluster TO the t2 cluster or otherwise desired location. It has the same functionality as `kube_copy.py`. For example:

```bash
# Dry run first
python kube_reversecopy.py \
  --input-dirs /data/ADsamples/VBFHto2B_25 \
  --output-path /ceph/cms/store/user/username/ntuples/VBFHto2B_25 \
  --namespace axol1tl \
  --pvc traindatavol \
  --copy-pod copy-pod \
  --dry-run

# For real
python kube_reversecopy.py \
  --input-dirs /data/ADsamples/VBFHto2B_25 \
  --output-path /ceph/cms/store/user/username/ntuples/VBFHto2B_25 \
  --namespace axol1tl \
  --pvc traindatavol \
  --copy-pod copy-pod \
  --files-per-job 50 \
  --max-parallel 4 \
  --flat
```

The full list of options can be found below. the only meaningful differences compared to `kube_copy.py` are:

* `--input-dirs` describes PVC paths instead of UAF paths
* `--output-path` describes a UAF destination instead of a PVC destination
* `--skip-existing` says "present locally on UAF" instead of "present in the pod"
* `--dry-run` notes that file discovery requires a live pod, so it exits early

Reverse copy options:

| Flag | Default | Description |
|---|---|---|
| `--input-dirs` | required | One or more source directories inside the NRP PVC (e.g. `/data/ntuples/QCD`). Recursively finds all `.root` files via `kubectl exec`. |
| `--output-path` | required | Destination directory on UAF (e.g. `/ceph/cms/store/user/username/ntuples`). |
| `--namespace` | `axol1tl` | Kubernetes namespace. |
| `--pvc` | required | PVC name, e.g. `username-pvc`. |
| `--copy-pod` | `copy-pod` | Name of the long-lived pod with the PVC mounted. |
| `--create-pod` | off | Create the copy pod if it doesn't exist. |
| `--prefix` | none | One prefix string per input dir. `--prefix QCD TTbar` renames files to `QCD_file.root`, `TTbar_file.root`. Count must match `--input-dirs`. |
| `--filetype` | `*.root` | File pattern to match. e.g. `--filetype '*.h5'` or `--filetype '*'` for all files. |
| `--flat` | off | Put all output files in one flat directory. Without this, subdirectory structure from the PVC is preserved. |
| `--files-per-job` | `100` | Number of files per batch. |
| `--max-parallel` | `4` | Maximum number of batches running simultaneously. |
| `--skip-existing` | off | Skip files already present locally on UAF. |
| `--no-wait` | off | Launch batches and return immediately. Use `--summarize` to check results later. |
| `--summarize` | off | Parse log files and print summary. No copying. Pass log glob: `--summarize copy_logs/batch_*.log`. Also needs `--output-path`, `--namespace`, `--pvc`, `--copy-pod` for the resubmit command. |
| `--krsync` | `./krsync` | Path to krsync wrapper. Created automatically if missing. |
| `--log-dir` | `./copy_logs` | Directory for per-batch shell scripts and log files. |
| `--log-file` | `copy_summary.json` | JSON file summarising all file statuses at the end of a blocking run. |
| `--dry-run` | off | Print everything that would happen without copying anything. Note: file discovery requires a live pod connection, so dry-run exits after the pod check. |

## Exposing/publishing a NRP volume via HTTP (Sidenote)

Also included in the `ymls/webservices` directory are example yamls for creating a website that shows the contents of the pvc (traindatavol in this example, or where the files are) on the kubernetes cluster. Please refer to the [NRP documentation](https://nrp.ai/documentation/userdocs/tutorial/basic2/) for more information.

The website can be created within the webservices directory via for example:

```
kubectl create -f traindatavol-server-dep.yml -n namespace
kubectl create -f traindatavol-expose.yml -n namespace
kubectl create -f traindatavol-ingress.yml -n namespace
```

This the contents of the pvc can then be viewed online at the associated http address, for example `traindatavol` here: https://traindatavol.nrp-nautilus.io

Files can then be viewed and downloaded directly from there as needed.

This is by no means required but may be a useful visualization for large projects.

Note by default these websites are killed every 2 weeks, if you need them to last longer ask in the NRP matrix chat.