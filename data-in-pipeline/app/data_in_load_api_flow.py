"""Simple flow to test VPC Ingress Connection to the data-in-load-api service.

This flow is designed for quick iteration without requiring Docker builds.
"""

import ipaddress
import socket
from urllib.parse import urlparse

import requests
from prefect import flow, task


def _resolve_domain(domain: str) -> str | None:
    """Resolve domain to IP address."""
    try:
        resolved_ip = socket.gethostbyname(domain)
        print(f"DNS: {domain} -> {resolved_ip}")

        ip_obj = ipaddress.ip_address(resolved_ip)
        if ip_obj.is_private and resolved_ip.startswith("10.0."):
            print("  Private VPC IP (10.0.x.x)")
        elif ip_obj.is_private:
            print("  WARNING: Private but not in 10.0.x.x range")
        else:
            print("  ERROR: PUBLIC IP - VPC Ingress Connection not working")

        return resolved_ip
    except socket.gaierror as e:
        print(f"DNS failed for {domain}: {e}")
        return None


def _test_tcp(ip: str, port: int) -> bool:
    """Test TCP connection."""
    print(f"TCP {ip}:{port}...", end=" ")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        result = sock.connect_ex((ip, port))
        if result == 0:
            print("OK")
            return True
        print(f"FAILED (error {result})")
        return False
    except Exception as e:
        print(f"FAILED ({e})")
        return False
    finally:
        sock.close()


def _test_http(url: str) -> bool:
    """Test HTTP connectivity."""
    print(f"HTTP {url}...", end=" ")
    try:
        response = requests.get(url, timeout=30)
        print(f"OK ({response.status_code})")
        return True
    except requests.exceptions.Timeout:
        print("TIMEOUT")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"CONNECTION ERROR: {e}")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False


@task(log_prints=True)
def connectivity_test(url: str) -> dict:
    """Test connectivity to a URL."""
    results = {"url": url, "dns": False, "ip": None, "tcp": False, "http": False}

    parsed = urlparse(url)
    domain = parsed.netloc

    ip = _resolve_domain(domain)
    if ip:
        results["dns"] = True
        results["ip"] = ip
        results["tcp"] = _test_tcp(ip, 443)

    results["http"] = _test_http(url)
    return results


@flow(log_prints=True)
def test_data_in_load_api(url: str) -> dict:
    """Test connectivity to the data-in-load-api service via VPC Ingress Connection.

    Args:
        url: The full URL to test (e.g. https://xxx.eu-west-1.awsapprunner.com/)
    """
    print("=" * 50)
    print("VPC INGRESS CONNECTION TEST")
    print("=" * 50)

    results = connectivity_test(url)

    print("\n" + "=" * 50)
    print("RESULT:", "PASS" if results["http"] else "FAIL")
    print("=" * 50)

    return results
