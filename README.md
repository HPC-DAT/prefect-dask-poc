# JDS_development_template
Template repository for generating (development/exploration stage) repositories for use with jupyterdask tool (see JupyterDaskOnSlurm)

## Use of the template repository
This repository provides a simple template as basis for developing analysis that is to be deployed/run on an HPC system using the jupyterdask tool from the RS-DAT framework.

Starting from this repository users can create and manage their code. GitHub actions to provide containerized environments for deployment as well as a modifiable submission script for use with the jupyterdask cli tool are provided.

## Running a single Prefect flow

### Interactive use

```{bash}
apptainer pull dask_prefect.sif oras://ghcr.io/hpc-dat/prefect-dask-poc:latest
apptainer run dask_prefect.sif python /src/flow.py
```

### Slurm batch job
