import click
import os
from subprocess import run
import sys

from . import process_boundaries, process_blocks, generate_synthetic_people


@click.command()
def start():
    """Download US 2020 Census Data and convert to parquet files."""
    module_path = sys.modules['census_parquet'].__path__[0]

    click.echo('Stage 1: Download boundaries')
    run(os.path.join(module_path, 'download_boundaries.sh'))

    click.echo('Stage 2: Download population stats')
    run(os.path.join(module_path, 'download_population_stats.sh'))

    click.echo('Stage 3: Download blocks')
    run(os.path.join(module_path, 'download_blocks.sh'))

    click.echo('Stage 4: Process boundaries')
    process_boundaries.main()

    click.echo('Stage 5: Process blocks')
    process_blocks.main()

@click.command()
def synthetic_people():
    """Generate a point for each person within the census data."""
    click.echo('Generating Synthetic People')
    generate_synthetic_people.main()
