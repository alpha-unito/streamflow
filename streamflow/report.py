from __future__ import annotations

import argparse
import csv
import os

from streamflow.core.context import StreamFlowContext


def _export_to_file(fig, args: argparse.Namespace, dirname: str, i: int | None) -> None:
    import plotly.io as pio

    if "html" in args.format:
        path = f"{dirname}/report{f'_{i+1}' if i is not None else ''}.html"
        pio.write_html(fig, file=path)
        print(f"Report saved to {path}")
    if "json" in args.format:
        path = f"{dirname}/report{f'_{i+1}' if i is not None else ''}.json"
        pio.write_json(fig, file=path)
        print(f"Report saved to {path}")
    for f in (f for f in args.format if f not in ["csv", "html", "json"]):
        path = f"{dirname}/report{f'_{i+1}' if i is not None else ''}.{f}"
        pio.write_image(
            fig,
            format=f,
            file=path,
        )
        print(f"Report saved to {path}")


async def create_report(context: StreamFlowContext, args: argparse.Namespace):
    import pandas as pd
    import plotly.express as px

    # Retrieve data
    if reports := [
        r
        for r in await context.database.get_reports(
            args.workflow, last_only=not args.all
        )
        if r
    ]:
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
    # If workflow has no step, simply print a message and exit
    else:
        print(
            f"Workflow {args.workflow} did not execute any step: no report has been generated"
        )
