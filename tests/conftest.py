import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SHOP = ROOT / "demo" / "shop"
sys.path.insert(0, str(SHOP))

from shop import Shop  # noqa: E402

from k11_integration_qa.brokers import MemoryBroker  # noqa: E402


@pytest.fixture
def project(tmp_path):
    dest = tmp_path / "shop"
    shutil.copytree(SHOP, dest, ignore=shutil.ignore_patterns("__pycache__", "shop.db", "integration-report"))
    return dest


@pytest.fixture
def make_shop(project, monkeypatch):
    """make_shop(bugs="consumer") starts K11 Shop on the in-process broker, or on Kafka when kafka=True."""
    shops = []

    def make(bugs="", kafka=False):
        MemoryBroker.reset()
        monkeypatch.setenv("SHOP_BUGS", bugs)  # the batch job reads it too
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP") if kafka else None
        s = Shop(0, project / "shop.db", bugs, bootstrap)
        shops.append(s)
        monkeypatch.setenv("SHOP_URL", s.url)
        monkeypatch.setenv("QA_MCP_ROOT", str(project))
        return s

    yield make
    for s in shops:
        s.close()
