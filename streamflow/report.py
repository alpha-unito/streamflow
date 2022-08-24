import argparse
import csv
import datetime

from streamflow.core.context import StreamFlowContext


def _export_to_file(fig, args: argparse.Namespace, default_name: str) -> None:
    import plotly.io as pio
    if args.format == "html":
        pio.write_html(fig, file=args.name or default_name + ".html")
    elif args.format == "json":
        pio.write_json(fig, file=args.name or default_name + ".json")
    else:
        pio.write_image(fig, format=args.format, file=args.name or "{}.{}".format(default_name, args.format))


def _timestamp_to_datetime(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp / 1e9)
    return '{}{:03.0f}'.format(dt.strftime('%Y-%m-%dT%H:%M:%S.%f'), timestamp % 1e3)


async def create_report(context: StreamFlowContext,
                        args: argparse.Namespace):
    import plotly.express as px
    # Retrieve data
    report = await context.database.get_report(args.workflow)
    # If output format is csv, print DataFrame and exit
    if args.format == 'csv':
        with open(args.name or "report.csv", 'w') as f:
            writer = csv.DictWriter(f, report[0].keys())
            writer.writeheader()
            writer.writerows(report)
            return
    # Pre-process data
    import pandas as pd
    df = pd.DataFrame(data=report)
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
    _export_to_file(fig, args, "report")
