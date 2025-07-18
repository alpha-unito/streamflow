from __future__ import annotations

import argparse
import csv
import os

from streamflow.core.context import StreamFlowContext


def _export_to_file(fig, args: argparse.Namespace, path: str) -> None:
    import plotly.io as pio

    if "html" in args.format:
        fullpath = f"{path}.html"
        pio.write_html(fig, file=fullpath)
        print(f"Report saved to {fullpath}")
    if "json" in args.format:
        fullpath = f"{path}.json"
        pio.write_json(fig, file=fullpath)
        print(f"Report saved to {fullpath}")
    for f in (f for f in args.format if f not in ["csv", "html", "json"]):
        fullpath = f"{path}.{f}"
        pio.write_image(
            fig,
            format=f,
            file=fullpath,
        )
        print(f"Report saved to {fullpath}")


async def create_report(context: StreamFlowContext, args: argparse.Namespace) -> None:
    import pandas as pd
    import plotly.express as px

    # Retrieve data
    path = os.path.abspath(
        os.path.join(args.outdir or os.getcwd(), args.name or "report")
    )
    reports = []
    workflows = [w.strip() for w in args.workflows.split(",")]
    for workflow in workflows:
        if wf_reports := [
            r
            for r in await context.database.get_reports(
                workflow, last_only=not args.all
            )
            if r
        ]:
            for report in wf_reports:
                reports.extend(
                    [
                        {
                            **row,
                            **{
                                "name": (
                                    workflow + row["name"]
                                    if len(workflows) > 1
                                    else row["name"]
                                )
                            },
                        }
                        for row in report
                    ]
                )
        # If workflow has no step, simply print a message and exit
        else:
            print(
                f"Workflow {workflow} did not execute any step: no report has been generated"
            )
    # If output format is csv, print DataFrame
    if "csv" in args.format:
        with open(f"{path}.csv", "w") as f:
            writer = csv.DictWriter(f, reports[0].keys())
            writer.writeheader()
            writer.writerows(reports)
        print(f"Report saved to {path}.csv")
    # Pre-process data
    df = pd.DataFrame(data=reports)
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
    _export_to_file(fig, args, path)
