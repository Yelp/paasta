import subprocess

from behave import then


@then('we can successfully run command {command}')
def check_status_from_command(context, command):
    subprocess.check_call(command, shell=True)
