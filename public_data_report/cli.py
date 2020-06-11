import click

from .hardware_report import hardware_report
from .user_activity import user_activity


@click.group()
def entry_point():
    pass


entry_point.add_command(hardware_report.main, "hardware_report")
entry_point.add_command(user_activity.main, "user_activity")


if __name__ == "__main__":
    entry_point()
