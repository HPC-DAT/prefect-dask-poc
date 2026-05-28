"""
AHN LiDAR filtering PoC — Prefect orchestrating Dask on SLURM.

Demonstrates two complementary integration patterns on a single
SLURM-backed Dask cluster:

  1. Task-level parallelism — `read_chunk.map(...)` fans Prefect tasks
     out onto SLURM-allocated Dask workers.

  2. Collection-level parallelism — inside `build_and_filter`, we grab
     the runner's cluster via `prefect_dask.get_dask_client()` and run
     a dask-geopandas computation against the same workers (no second
     cluster is created).

Pipeline:
  download_tile -> plan_chunks -> read_chunk.map -> build_and_filter -> write_parquet
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Tuple
from urllib.request import urlretrieve

import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import laspy
import numpy as np
import pandas as pd
from dask_jobqueue import SLURMCluster
from prefect import flow, get_run_logger, task, unmapped
from prefect_dask import DaskTaskRunner, get_dask_client

# ---------------------------------------------------------------------------
# Uncomment to enable DEBUG logging
# ---------------------------------------------------------------------------
import logging
logging.getLogger("distributed").setLevel(logging.DEBUG)
logging.getLogger("dask_jobqueue").setLevel(logging.DEBUG)


# ---------------------------------------------------------------------------
# Config — Using Spider configuration.
# ---------------------------------------------------------------------------
APPTAINER_IMAGE="oras://ghcr.io/hpc-dat/prefect-dask-poc:latest"
SLURM_KWARGS = dict(
    queue="normal",
    cores=2,
    processes=1,          # one Dask worker process per SLURM job
    memory="16GB",
    walltime="02:00:00",
    death_timeout=60,
    job_extra_directives=["--output=.prefect_dask/%x-%j.out"],
    python=f"apptainer run {APPTAINER_IMAGE} python",
    # local_directory="\$TMPDIR",
    # job_script_prologue=[
    #     # Whatever it takes to activate the env on a worker node, e.g.:
    #     # "module load 2023",
    #     # "source /home/<you>/envs/prefect-dask-poc/bin/activate",
    #     "<FILL IN: env activation lines>",
    # ],
    # Graceful shutdown before walltime expires:
    # worker_extra_args=["--lifetime", "55m", "--lifetime-stagger", "4m"],
)
ADAPT_KWARGS = dict(minimum=2, maximum=8)

# Public AHN3/AHN4 LAZ tile. See README for sources.
DEFAULT_TILE_URL = "<FILL IN: https://.../some_tile.LAZ>"

CHUNK_POINTS = 5_000_000        # points per read_chunk task
CRS_AHN = "EPSG:28992"          # Amersfoort / RD New
KEEP_CLASSIFICATION = 2         # 2 = ground returns (ASPRS spec)
SAMPLE_FRAC = 0.01              # 1% sample, just to keep the demo light


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=10)
def download_tile(url: str, dest_dir: str) -> str:
    """Fetch the LAZ tile to a (shared) filesystem location."""
    log = get_run_logger()
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    dest = Path(dest_dir) / Path(url).name
    if dest.exists():
        log.info("Reusing cached tile at %s", dest)
    else:
        log.info("Downloading %s -> %s", url, dest)
        urlretrieve(url, dest)
    return str(dest)


@task
def plan_chunks(laz_path: str, chunk_points: int = CHUNK_POINTS) -> List[Tuple[int, int]]:
    """Return [(start, count), ...] covering every point in the file."""
    with laspy.open(laz_path) as f:
        n = f.header.point_count
    return [(i, min(chunk_points, n - i)) for i in range(0, n, chunk_points)]


@task
def read_chunk(laz_path: str, start: int, count: int) -> pd.DataFrame:
    """Read a contiguous slice of points from the LAZ file."""
    with laspy.open(laz_path) as f:
        f.seek(start)
        pts = f.read_points(count)
    return pd.DataFrame(
        {
            "X": np.asarray(pts.x),
            "Y": np.asarray(pts.y),
            "Z": np.asarray(pts.z),
            "intensity": np.asarray(pts.intensity),
            "classification": np.asarray(pts.classification),
        }
    )


@task
def build_and_filter(
    chunks: List[pd.DataFrame],
    keep_class: int,
    sample_frac: float,
) -> gpd.GeoDataFrame:
    """
    Concatenate chunk DataFrames into a dask-geopandas GeoDataFrame, filter
    by classification, sample, and materialise.

    Runs on the runner's SLURM cluster via `get_dask_client()` — this is the
    "collection-level parallelism" half of the demo.
    """
    log = get_run_logger()
    with get_dask_client():
        ddf = dd.from_pandas(
            pd.concat(chunks, ignore_index=True),
            npartitions=max(1, len(chunks)),
        )
        ddf = ddf[ddf["classification"] == keep_class]
        geometry = dask_geopandas.points_from_xy(ddf, x="X", y="Y", crs=CRS_AHN)
        gddf = dask_geopandas.from_dask_dataframe(ddf, geometry=geometry)
        result = gddf.sample(frac=sample_frac, random_state=0).compute()
    log.info("Filtered+sampled point count: %d", len(result))
    return result


@task
def write_parquet(gdf: gpd.GeoDataFrame, out_path: str) -> str:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(out_path)
    return out_path


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="ahn-filtering",
    task_runner=DaskTaskRunner(
        cluster_class=SLURMCluster,
        cluster_kwargs=SLURM_KWARGS,
        adapt_kwargs=ADAPT_KWARGS,
    ),
)
def ahn_filtering(
    tile_url: str = DEFAULT_TILE_URL,
    work_dir: str = "./work",
    out_path: str = "./work/ground_sample.parquet",
) -> str:
    laz_path_fut = download_tile.submit(tile_url, work_dir)
    chunks_plan_fut = plan_chunks.submit(laz_path_fut)

    # Resolve the chunk plan so we can expand it into a .map() call.
    chunks_list = chunks_plan_fut.result()
    starts = [s for s, _ in chunks_list]
    counts = [c for _, c in chunks_list]

    # (1) Task-level parallelism — each read runs as a Prefect task on a worker.
    chunk_dfs = read_chunk.map(
        laz_path=unmapped(laz_path_fut),
        start=starts,
        count=counts,
    )

    # (2) Collection-level parallelism — dask-geopandas on the same cluster.
    filtered = build_and_filter.submit(chunk_dfs, KEEP_CLASSIFICATION, SAMPLE_FRAC)

    return write_parquet.submit(filtered, out_path).result()


if __name__ == "__main__":
    ahn_filtering()
