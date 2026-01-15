"""Cluster address resolution and management."""

import socket
from typing import List, Optional, Tuple


class Address:
    """Represents a network address for a Hazelcast cluster member."""

    DEFAULT_PORT = 5701

    def __init__(self, host: str, port: int = DEFAULT_PORT):
        self._host = host
        self._port = port
        self._resolved_addresses: Optional[List[Tuple[str, int]]] = None

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    def resolve(self) -> List[Tuple[str, int]]:
        """Resolve the hostname to IP addresses.

        Returns:
            List of (ip_address, port) tuples.
        """
        if self._resolved_addresses is not None:
            return self._resolved_addresses

        try:
            infos = socket.getaddrinfo(
                self._host,
                self._port,
                socket.AF_UNSPEC,
                socket.SOCK_STREAM,
            )
            self._resolved_addresses = []
            seen = set()
            for info in infos:
                addr = info[4][:2]
                if addr not in seen:
                    seen.add(addr)
                    self._resolved_addresses.append(addr)
        except socket.gaierror:
            self._resolved_addresses = [(self._host, self._port)]

        return self._resolved_addresses

    def invalidate_cache(self) -> None:
        """Invalidate the resolved address cache."""
        self._resolved_addresses = None

    def __str__(self) -> str:
        return f"{self._host}:{self._port}"

    def __repr__(self) -> str:
        return f"Address({self._host!r}, {self._port})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Address):
            return False
        return self._host == other._host and self._port == other._port

    def __hash__(self) -> int:
        return hash((self._host, self._port))


class AddressHelper:
    """Utility class for parsing and resolving cluster addresses."""

    @staticmethod
    def parse(address_string: str) -> Address:
        """Parse an address string into an Address object.

        Args:
            address_string: Address in format "host:port" or "host".

        Returns:
            Address object.
        """
        address_string = address_string.strip()

        if address_string.startswith("["):
            bracket_end = address_string.find("]")
            if bracket_end > 0:
                host = address_string[1:bracket_end]
                rest = address_string[bracket_end + 1 :]
                if rest.startswith(":"):
                    port = int(rest[1:])
                else:
                    port = Address.DEFAULT_PORT
                return Address(host, port)

        if ":" in address_string:
            colon_count = address_string.count(":")
            if colon_count == 1:
                host, port_str = address_string.rsplit(":", 1)
                try:
                    port = int(port_str)
                    return Address(host, port)
                except ValueError:
                    return Address(address_string, Address.DEFAULT_PORT)
            else:
                return Address(address_string, Address.DEFAULT_PORT)

        return Address(address_string, Address.DEFAULT_PORT)

    @staticmethod
    def parse_list(address_strings: List[str]) -> List[Address]:
        """Parse a list of address strings.

        Args:
            address_strings: List of address strings.

        Returns:
            List of Address objects.
        """
        return [AddressHelper.parse(addr) for addr in address_strings]

    @staticmethod
    def get_possible_addresses(
        addresses: List[Address], port_range: int = 3
    ) -> List[Address]:
        """Generate possible addresses including port variations.

        Args:
            addresses: Base addresses to expand.
            port_range: Number of consecutive ports to try.

        Returns:
            Expanded list of addresses.
        """
        result = []
        seen = set()

        for addr in addresses:
            for port_offset in range(port_range):
                new_addr = Address(addr.host, addr.port + port_offset)
                if new_addr not in seen:
                    seen.add(new_addr)
                    result.append(new_addr)

        return result
