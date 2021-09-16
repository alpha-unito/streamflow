import argparse
import os.path

import pandas as pd
import plotly.express as px
import plotly.io as pio

from streamflow.persistence.persistence_manager import DefaultPersistenceManager
from streamflow.persistence.sqlite import SqliteDatabase


def create_report(args: argparse.Namespace):
    # Retrieve data
    database = SqliteDatabase(os.path.join(args.outdir, ".streamflow", "sqlite.db"), reset_db=False)
    persistence_manager = DefaultPersistenceManager(db=database, output_dir=args.outdir)
    df = persistence_manager.db.get_report()

    # If output format is csv, print DataFrame and exit
    if args.format == 'csv':
        df.to_csv(args.name or "report.csv", index=False)
        return

    # Pre-process data
    df["id"] = df["id"].map(str)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])

    # Create chart
    fig = px.timeline(
        df,
        x_start="start_time",
        x_end="end_time",
        y="name" if args.group_by_step else "id",
        color="name")
    fig.update_yaxes(visible=False)

    # Export to file
    if args.format == "html":
        pio.write_html(fig, file=args.name or "report.html")
    elif args.format == "json":
        pio.write_json(fig, file=args.name or "reprot.json")
    else:
        pio.write_image(fig, format=args.format, file=args.name or "report.{}".format(args.format))
