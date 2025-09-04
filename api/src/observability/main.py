import argparse
import os
import sys 

from grafana_foundation_sdk.builders import dashboard
from grafana_foundation_sdk.cog.encoder import JSONEncoder

from observability.dashboards.root import root_dashboard
from observability.registry import ServiceRegistry

MANIFESTS_DIR="./resources"
DASHBOARD_FOLDER_NAME="WIP"

if __name__ == "__main__":
    registry = ServiceRegistry()

    dash = root_dashboard(registry).build()
    encoder = JSONEncoder(sort_keys=True, indent=2)

    print(encoder.encode(dash))
