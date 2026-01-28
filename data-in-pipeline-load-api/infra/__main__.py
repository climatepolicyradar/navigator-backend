import pulumi

config = pulumi.Config()
environment = pulumi.get_stack()
name = pulumi.get_project()

tags = {
    "CPR-Created-By": "pulumi",
    "CPR-Pulumi-Stack-Name": environment,
    "CPR-Pulumi-Project-Name": pulumi.get_project(),
    "CPR-Tag": f"{environment}-{name}-store",
    "Environment": environment,
}
