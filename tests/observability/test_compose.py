"""The local viewing stack's files are consistent (the stack itself needs Docker)."""

from pathlib import Path

import yaml

STACK = Path(__file__).parents[2] / "observability"


def test_compose_stack_wires_marquez_tempo_and_grafana() -> None:
    compose = yaml.safe_load((STACK / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]
    assert {"marquez-api", "marquez-web", "marquez-db", "tempo", "grafana"} <= set(services)
    assert "5000:5000" in services["marquez-api"]["ports"]  # GUARDIAN_OPENLINEAGE_URL
    assert "4318:4318" in services["tempo"]["ports"]  # OTEL_EXPORTER_OTLP_ENDPOINT
    for service in services.values():  # every mounted config file exists
        for volume in service.get("volumes", []):
            source = volume.split(":")[0]
            if source.startswith("./"):
                assert (STACK / source).is_file(), source
    tempo = yaml.safe_load((STACK / "tempo.yaml").read_text(encoding="utf-8"))
    assert tempo["distributor"]["receivers"]["otlp"]["protocols"]["http"]["endpoint"].endswith(
        ":4318"
    )
    sources = yaml.safe_load((STACK / "grafana" / "datasources.yaml").read_text(encoding="utf-8"))
    assert sources["datasources"][0]["url"] == "http://tempo:3200"
