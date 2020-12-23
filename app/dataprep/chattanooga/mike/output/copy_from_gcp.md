```bash
gcloud compute scp --recurse\
    carta-occupancy2:/home/jupyter/Juan/transit-occupancy-dashboard-1/app/dataprep/chattanooga/mike/output \
    /Volumes/external_drive/Code/transit-occupancy-dashboard/app/dataprep/chattanooga/mike/output \
    --zone=us-central1-a \
    --project "carta-occupancy"
```