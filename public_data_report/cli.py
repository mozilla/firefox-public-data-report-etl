import click

from .hardware_report import hardware_report


@click.group()
def entry_point():
    pass


entry_point.add_command(hardware_report.main, "hardware_report")


if __name__ == "__main__":
    entry_point()
