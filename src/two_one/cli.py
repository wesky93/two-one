import os

import click
from neomodel import config


def set_db_config():
    NEO4J_USERNAME = os.environ.get('NEO4J_USERNAME', 'neo4j')
    NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')
    NEO4J_HOST = os.environ.get('NEO4J_HOST', 'localhost')
    NEO4J_PORT = os.environ.get('NEO4J_PORT', 7687)

    config.AUTO_INSTALL_LABELS = False
    config.DATABASE_URL = f'bolt://{NEO4J_USERNAME}:{NEO4J_PASSWORD}@{NEO4J_HOST}:{NEO4J_PORT}'


@click.group()
def cli():
    pass


@cli.group()
def ingest():
    pass


@ingest.command()
def dashboard():
    from collector.models import NeoDashboard
    NeoDashboard.ingest_dashboard()


@ingest.command()
def reset():
    from neomodel import db
    set_db_config()
    db.cypher_query("MATCH (r:Resource) DETACH DELETE r")


@ingest.command()
@click.option('--bucket', prompt='vpc flow log bucket')
@click.option('--prefix', prompt='vpc flow log prefix')
def vpc_flow_logs(bucket: str, prefix: str, ):
    from .collector.handler import start_ingest

    set_db_config()
    start_ingest(bucket, prefix)


@cli.group()
def server():
    pass


@server.command()
def start():
    '''
    start neodahs &
    :return:
    '''
    pass


if __name__ == '__main__':
    cli()