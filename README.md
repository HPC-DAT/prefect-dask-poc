# JDS_development_template
Template repository for generating (development/exploration stage) repositories for use with jupyterdask tool (see JupyterDaskOnSlurm)

## Use of the template repository
This repository provides a simple template as basis for developing analysis that is to be deployed/run on an HPC system using the jupyterdask tool from the RS-DAT framework.

Starting from this repository users can create and manage their code. GitHub actions to provide containerized environments for deployment as well as a modifiable submission script for use with the jupyterdask cli tool are provided.

## Running a single Prefect flow

### Interactive use

```{bash}
srun -n 1 -c 4 -t 1:00:00 --pty /bin/bash
apptainer pull dask_prefect.sif oras://ghcr.io/hpc-dat/prefect-dask-poc:latest

# Use reference to apptainer image, so image is not pulled on each node
export APPTAINER_IMAGE=$(realpath dask_prefect.sif)

# Bind all paths from host system within the container
_DIRS=`/usr/bin/ls -1 / | /usr/bin/awk '!/dev/' | /usr/bin/sed 's/^/\//g' `
export APPTAINER_BIND=`echo ${_DIRS} | /usr/bin/sed 's/ /,/g' `

# Prefect config
# Select free port and host ip
export PREFECT_API_PORT=`comm -23 <(seq 49152 65535 | sort) <(ss -Htan | awk '{print $4}' | cut -d':' -f2 | sort -u) | shuf | head -n 1`
export PREFECT_API_URL="http://$(hostname -I | awk '{print $1}'):${PREFECT_API_PORT}/api"
# Turn off Prefect tracking
export DO_NOT_TRACK=1
# Prefect log level
export PREFECT_LOGGING_LEVEL=DEBUG

# Start local Prefect server
export PREFECT_SERVER_API_HOST=$(hostname -I | awk '{print $1}')
export PREFECT_SERVER_API_PORT=$PREFECT_API_PORT
apptainer run $APPTAINER_IMAGE prefect server start --no-ui &

# Run prefect pipeline in script
apptainer run $APPTAINER_IMAGE python /src/flow.py
```

### Slurm batch job


## Disclaimer

PoC developed with Claude Code.
