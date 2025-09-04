"""
The registry is used to catalog the services and key resources that are monitored.

This is then used to generate the dashboards and alerts.

Version 1 (2025-09-02): 
- Starting with very simple registry:
- looks for service manifests in subdirectories for service definitions
- assumes all services are fastapi and auto-instrumented
- also supports a 'critical' endpoint annotation for endpoints that 
we want to have bespoke care for -- e.g. search, config, etc. 
"""

from api.telemetry_config import ServiceManifest
from pathlib import Path

class ServiceRegistry:
    """Registry of services and key resources"""

    def __init__(self):
        self.services = self.find_services()

    def find_services(self) -> list[ServiceManifest]:
        """Find all service manifests in the subdirectories"""
        services = []

        for path in Path(".").glob("*/service-manifest.json"):
            services.append(ServiceManifest.from_file(path))

        return services

    def get_services(self) -> list[ServiceManifest]:
        """Get all services"""
        return self.services
    
    def __str__(self) -> str:   
        """String representation of the registry"""
        return "\n".join([str(service.model_dump()) for service in self.services])

    def __repr__(self) -> str:
        """String representation of the registry"""
        return self.__str__()
