# GCP Deployment — Single VM

Keeps infra minimal on purpose: one Compute Engine VM runs Kafka (Docker) and
the Spark job. Cloud Storage holds the streaming checkpoint directory. No GKE,
no Dataproc — the trial's constraint is time, not compute budget.

## 1. Create the VM

```bash
gcloud compute instances create pulseops-vm \
  --zone=us-central1-a \
  --machine-type=e2-standard-4 \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --boot-disk-size=50GB
```

## 2. Install Docker + Java + Python on the VM

```bash
curl -fsSL https://get.docker.com | sh
sudo apt-get install -y openjdk-17-jre-headless python3-pip python3-venv unzip
```

## 3. Create a GCS bucket for Spark checkpoints

```bash
gsutil mb -l us-central1 gs://your-bucket-name
```

Set `GCS_CHECKPOINT_PATH=gs://your-bucket-name/pulseops/checkpoints` in `.env`.

Note: to have Spark write checkpoints directly to GCS you'll need the GCS
connector JAR (`gcs-connector-hadoop3-latest.jar`) on the Spark classpath. For
the MVP, using a local disk path for checkpoints (default in `.env.example`)
is a perfectly reasonable simplification — swap to GCS once the pipeline is
proven end-to-end, and note the trade-off in the README's "Challenges &
Solutions" section.

## 4. Clone the repo, install dependencies, and run

```bash
git clone <your-repo-url>
cd pulseops
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
bash data/download_hdfs_dataset.sh
docker compose up -d
```

Then follow the setup order in the main README.
