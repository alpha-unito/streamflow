from __future__ import annotations

import argparse
import csv
import os

from streamflow.core.context import StreamFlowContext


def _export_to_file(fig, args: argparse.Namespace, dirname: str, i: int | None) -> None:
    import plotly.io as pio

    if "html" in args.format:
        pio.write_html(
            fig, file=f"{dirname}/report{f'_{i+1}' if i is not None else ''}.html"
        )
    if "json" in args.format:
        pio.write_json(
            fig, file=f"{dirname}/report{f'_{i+1}' if i is not None else ''}.json"
        )
    for f in (f for f in args.format if f not in ["csv", "html", "json"]):
        pio.write_image(
            fig,
            format=f,
            file=f"{dirname}/report{f'_{i+1}' if i is not None else ''}.{f}",
        )


async def create_report(context: StreamFlowContext, args: argparse.Namespace):
    import pandas as pd
    import plotly.express as px

    # Retrieve data
    reports = await context.database.get_reports(args.workflow, last_only=not args.all)
    # Create report directory
    dirname = os.path.abspath(f"{args.name or f'{args.workflow}-report'}/")
    os.makedirs(dirname, exist_ok=True)
    # Create reports
    for i, report in enumerate(reports):
        # If output format is csv, print DataFrame
        if "csv" in args.format:
            with open(
                f"{dirname}/report{f'_{i+1}' if len(reports) > 1 else ''}.csv", "w"
            ) as f:
                writer = csv.DictWriter(f, report[0].keys())
                writer.writeheader()
                writer.writerows(report)
                # If no other formats are required, continue
                if len(args.format) == 1:
                    continue
        # Pre-process data
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
            color="name",
        )
        fig.update_yaxes(visible=False)
        # Export to file
        _export_to_file(fig, args, dirname, i if len(reports) > 1 else None)
