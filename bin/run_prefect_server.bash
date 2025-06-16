# if no prefect server, start one
prefect server start &

# This will open the Prefect dashboard in your browser at http://localhost:4200.

# if you have other prefect service which need to connect to the server api, you may need to setup
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

# check work pool
prefect work-pool ls

# Create a Process work pool if the desired work pool does not exist:
prefect work-pool create --type process casd-work-pool

# Start a worker to poll the work pool:
prefect worker start --pool casd-work-pool

# activate a deployment
python sample_deployment.py