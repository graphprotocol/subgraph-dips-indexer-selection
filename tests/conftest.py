"""
Shared pytest fixtures and custom Faker providers.

See:
- https://docs.pytest.org/en/6.2.x/fixture.html#conftest-py-sharing-fixtures-across-multiple-files
- https://faker.readthedocs.io/en/master/pytest-fixtures.html
"""

import os
import random
import string
from datetime import timedelta
from typing import Optional

import pytest
from faker import Faker
from faker.providers import BaseProvider, geo, internet
from faker_airtravel import AirTravelProvider
from hypothesis import HealthCheck, settings

from iisa.indexer_selection import (
    DeploymentId,
    EthAddressStr,
    IndexerId,
    IpfsHashStr,
    QueryIdStr,
)

# Property tests: few enough examples to keep the suite fast, a fixed seed on CI so a failure
# there repeats, and HYPOTHESIS_PROFILE=thorough for a much deeper search locally.
settings.register_profile(
    "fast",
    max_examples=50,
    deadline=timedelta(seconds=2),
    print_blob=True,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.register_profile(
    "ci", parent=settings.get_profile("fast"), derandomize=True, database=None
)
settings.register_profile("thorough", parent=settings.get_profile("fast"), max_examples=2_000)
settings.load_profile(
    os.environ.get("HYPOTHESIS_PROFILE", "ci" if os.environ.get("CI") else "fast")
)


class CustomFakerProvider(BaseProvider):
    """Domain-specific Faker providers for generating test data."""

    def query_id(self, iata_code: Optional[str] = None) -> QueryIdStr:
        """Generate a random query ID in the format "[a-f0-9]{16}-{IATA}"."""
        prefix = "".join(random.choices(string.hexdigits.lower(), k=16))
        suffix = iata_code or self.generator.airport_iata()
        return QueryIdStr(f"{prefix}-{suffix}")

    def eth_address(self) -> EthAddressStr:
        """Generate a random Ethereum address."""
        prefix = "0x"
        characters = string.hexdigits.lower()
        address = prefix + "".join(random.choices(characters, k=40))
        return EthAddressStr(address)

    def ipfs_hash(self) -> IpfsHashStr:
        """Generate a random IPFS hash (CIDv0)."""
        prefix = "Qm"
        characters = string.ascii_letters + string.digits
        cid = prefix + "".join(random.choices(characters, k=44))
        return IpfsHashStr(cid)

    def indexer_id(self) -> IndexerId:
        """Generate a random Indexer ID."""
        return IndexerId(self.eth_address())

    def deployment_id(self) -> DeploymentId:
        """Generate a random subgraph deployment ID."""
        return DeploymentId(self.ipfs_hash())


@pytest.fixture(scope="session", autouse=True)
def faker() -> Faker:
    """Create a Faker instance with domain-specific providers."""
    faker = Faker()
    faker.add_provider(geo)
    faker.add_provider(internet)
    faker.add_provider(AirTravelProvider)
    faker.add_provider(CustomFakerProvider)
    return faker
