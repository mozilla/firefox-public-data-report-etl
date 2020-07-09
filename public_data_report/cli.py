import click

from .hardware_report import hardware_report
from .user_activity import user_activity
from .annotations import annotations


@click.group()
def entry_point():
    pass


entry_point.add_command(hardware_report.main, "hardware_report")
entry_point.add_command(user_activity.main, "user_activity")
entry_point.add_command(annotations.main, "annotations")


if __name__ == "__main__":
    entry_point()
