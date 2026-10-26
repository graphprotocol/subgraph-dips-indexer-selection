# Project Instructions

## Deployment Reminder

After merging any PR that modifies files in `cronjobs/compute_scores/**` or `.github/workflows/rebuild-cronjob-image.yml`:

1. Wait for the "Rebuild CronJob Image" workflow to complete
2. Remind the user to run:
   ```bash
   kubectl apply -f k8s/score-computation-cronjob.yaml -f k8s/sync-status-fetcher.yaml
   ```

The workflow automatically updates `k8s/score-computation-cronjob.yaml` and `k8s/sync-status-fetcher.yaml` with the new image SHA tag, since both run the same image, but the user must manually apply them to the cluster.
