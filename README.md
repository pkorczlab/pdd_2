# PDD2

## Files

- `terraform/` provisions 1 central node in Poland and 4 agent nodes in US, Poland, Japan and Australia.
- `ansible/` installs Kafka, creates topics, copies scripts and creates `run_agent.sh` / `run_central.sh`.
- `scripts/agent.py` runs on every agent node.
- `scripts/central.py` runs on the central node.
- `scripts/plot_logs.py` creates line plots from fetched JSONL logs.
- `RESULTS.md` is the completed run report with log validation and observations.

## Deployment

The project id, SSH user and SSH public key are already set in `terraform/terraform.tfvars`:

- project: `pdd2-496716`
- SSH user: `piotr`
- SSH public key: `~/.ssh/pdd2_stun.pub`

Provision the machines:

```bash
cd terraform
terraform init
terraform apply
terraform output -json nodes
```

Copy `ansible/inventory.ini.example` to `ansible/inventory.ini` and fill public and internal IPs from the Terraform output.

Configure all machines:

```bash
cd ../ansible
ansible-playbook -i inventory.ini site.yml
```

Start scripts manually:

```bash
ansible agents -i inventory.ini -a /opt/stun-monitor/run_agent.sh
ansible central -i inventory.ini -a /opt/stun-monitor/run_central.sh
```

Let it run for at least one hour. Then fetch logs:

```bash
ansible-playbook -i inventory.ini fetch_logs.yml
```

Create plots locally:

```bash
cd ..
python3 -m venv .venv
. .venv/bin/activate
pip install -r scripts/requirements-plot.txt
python scripts/plot_logs.py --input run_logs --output plots
```

The plotting logs store one value per metric/server approximately every 72 seconds, which is 2% of a one-hour runtime.

## Kafka topics

Every agent has its own single-node Kafka broker and creates:

- `stun_metrics`
- `stun_availability`
- `stun_avg_latency`
- `stun_avg_availability`
- `stun_bottom_ten_percent_latency`
- `stun_global_availability`

The central node consumes `stun_availability` from every agent broker and writes `stun_global_availability` to the central broker.

## Cleanup

```bash
cd terraform
terraform destroy
```
