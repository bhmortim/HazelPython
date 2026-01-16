"""Unit tests for hazelcast.network.address module."""

import pytest
import socket
from unittest.mock import Mock, patch

from hazelcast.network.address import Address, AddressHelper


class TestAddress:
    """Tests for Address class."""

    def test_default_port(self):
        assert Address.DEFAULT_PORT == 5701

    def test_creation_with_default_port(self):
        addr = Address("localhost")
        assert addr.host == "localhost"
        assert addr.port == 5701

    def test_creation_with_custom_port(self):
        addr = Address("localhost", 5702)
        assert addr.host == "localhost"
        assert addr.port == 5702

    def test_host_property(self):
        addr = Address("192.168.1.1", 5701)
        assert addr.host == "192.168.1.1"

    def test_port_property(self):
        addr = Address("localhost", 5703)
        assert addr.port == 5703

    def test_resolve_caches_result(self):
        addr = Address("localhost")
        
        with patch("socket.getaddrinfo") as mock_getaddrinfo:
            mock_getaddrinfo.return_value = [
                (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701))
            ]
            
            result1 = addr.resolve()
            result2 = addr.resolve()
            
            assert result1 == result2
            assert mock_getaddrinfo.call_count == 1

    def test_resolve_returns_ip_port_tuples(self):
        addr = Address("localhost")
        
        with patch("socket.getaddrinfo") as mock_getaddrinfo:
            mock_getaddrinfo.return_value = [
                (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701)),
                (socket.AF_INET6, socket.SOCK_STREAM, 0, "", ("::1", 5701, 0, 0)),
            ]
            
            result = addr.resolve()
            
            assert ("127.0.0.1", 5701) in result
            assert ("::1", 5701) in result

    def test_resolve_deduplicates(self):
        addr = Address("localhost")
        
        with patch("socket.getaddrinfo") as mock_getaddrinfo:
            mock_getaddrinfo.return_value = [
                (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701)),
                (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701)),
            ]
            
            result = addr.resolve()
            
            assert len(result) == 1

    def test_resolve_handles_unresolvable(self):
        addr = Address("nonexistent.invalid.host")
        
        with patch("socket.getaddrinfo") as mock_getaddrinfo:
            mock_getaddrinfo.side_effect = socket.gaierror("Name resolution failed")
            
            result = addr.resolve()
            
            assert result == [("nonexistent.invalid.host", 5701)]

    def test_invalidate_cache(self):
        addr = Address("localhost")
        
        with patch("socket.getaddrinfo") as mock_getaddrinfo:
            mock_getaddrinfo.return_value = [
                (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701))
            ]
            
            addr.resolve()
            addr.invalidate_cache()
            addr.resolve()
            
            assert mock_getaddrinfo.call_count == 2

    def test_invalidate_cache_clears_resolved(self):
        addr = Address("localhost")
        addr._resolved_addresses = [("127.0.0.1", 5701)]
        
        addr.invalidate_cache()
        
        assert addr._resolved_addresses is None

    def test_str_representation(self):
        addr = Address("localhost", 5701)
        assert str(addr) == "localhost:5701"

    def test_str_representation_custom_port(self):
        addr = Address("192.168.1.1", 5702)
        assert str(addr) == "192.168.1.1:5702"

    def test_repr_representation(self):
        addr = Address("localhost", 5701)
        assert repr(addr) == "Address('localhost', 5701)"

    def test_equality_same(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        assert addr1 == addr2

    def test_equality_different_host(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("127.0.0.1", 5701)
        assert addr1 != addr2

    def test_equality_different_port(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5702)
        assert addr1 != addr2

    def test_equality_non_address(self):
        addr = Address("localhost", 5701)
        assert addr != "localhost:5701"
        assert addr != None
        assert addr != 5701

    def test_hash_same_addresses(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        assert hash(addr1) == hash(addr2)

    def test_hash_different_addresses(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5702)
        assert hash(addr1) != hash(addr2)

    def test_hash_usable_in_set(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        addr3 = Address("localhost", 5702)
        
        s = {addr1, addr2, addr3}
        assert len(s) == 2

    def test_hash_usable_in_dict(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5702)
        
        d = {addr1: "first", addr2: "second"}
        assert d[Address("localhost", 5701)] == "first"


class TestAddressHelper:
    """Tests for AddressHelper class."""

    def test_parse_host_only_uses_default_port(self):
        addr = AddressHelper.parse("localhost")
        assert addr.host == "localhost"
        assert addr.port == Address.DEFAULT_PORT

    def test_parse_host_with_port(self):
        addr = AddressHelper.parse("localhost:5702")
        assert addr.host == "localhost"
        assert addr.port == 5702

    def test_parse_ip_address(self):
        addr = AddressHelper.parse("192.168.1.1:5701")
        assert addr.host == "192.168.1.1"
        assert addr.port == 5701

    def test_parse_strips_whitespace(self):
        addr = AddressHelper.parse("  localhost:5701  ")
        assert addr.host == "localhost"
        assert addr.port == 5701

    def test_parse_ipv6_address_with_brackets(self):
        addr = AddressHelper.parse("[::1]")
        assert addr.host == "::1"
        assert addr.port == Address.DEFAULT_PORT

    def test_parse_ipv6_with_port(self):
        addr = AddressHelper.parse("[::1]:5702")
        assert addr.host == "::1"
        assert addr.port == 5702

    def test_parse_ipv6_full_address(self):
        addr = AddressHelper.parse("[2001:db8::1]:5703")
        assert addr.host == "2001:db8::1"
        assert addr.port == 5703

    def test_parse_invalid_port_treated_as_host(self):
        addr = AddressHelper.parse("localhost:abc")
        assert addr.host == "localhost:abc"
        assert addr.port == Address.DEFAULT_PORT

    def test_parse_multiple_colons_as_ipv6(self):
        addr = AddressHelper.parse("2001:db8::1")
        assert addr.host == "2001:db8::1"
        assert addr.port == Address.DEFAULT_PORT

    def test_parse_single_colon_with_valid_port(self):
        addr = AddressHelper.parse("myhost:9999")
        assert addr.host == "myhost"
        assert addr.port == 9999

    def test_parse_list(self):
        addresses = AddressHelper.parse_list([
            "localhost:5701",
            "192.168.1.1:5702",
            "node3",
        ])
        
        assert len(addresses) == 3
        assert addresses[0].host == "localhost"
        assert addresses[0].port == 5701
        assert addresses[1].host == "192.168.1.1"
        assert addresses[1].port == 5702
        assert addresses[2].host == "node3"
        assert addresses[2].port == 5701

    def test_parse_list_empty(self):
        addresses = AddressHelper.parse_list([])
        assert addresses == []

    def test_get_possible_addresses_expands_ports(self):
        base = [Address("localhost", 5701)]
        
        result = AddressHelper.get_possible_addresses(base, port_range=3)
        
        assert len(result) == 3
        assert Address("localhost", 5701) in result
        assert Address("localhost", 5702) in result
        assert Address("localhost", 5703) in result

    def test_get_possible_addresses_default_range(self):
        base = [Address("localhost", 5701)]
        
        result = AddressHelper.get_possible_addresses(base)
        
        assert len(result) == 3

    def test_get_possible_addresses_deduplicates(self):
        base = [
            Address("localhost", 5701),
            Address("localhost", 5702),
        ]
        
        result = AddressHelper.get_possible_addresses(base, port_range=3)
        
        unique_result = set((a.host, a.port) for a in result)
        assert len(unique_result) == len(result)

    def test_get_possible_addresses_multiple_hosts(self):
        base = [
            Address("host1", 5701),
            Address("host2", 5701),
        ]
        
        result = AddressHelper.get_possible_addresses(base, port_range=2)
        
        assert len(result) == 4
        assert Address("host1", 5701) in result
        assert Address("host1", 5702) in result
        assert Address("host2", 5701) in result
        assert Address("host2", 5702) in result

    def test_get_possible_addresses_empty_input(self):
        result = AddressHelper.get_possible_addresses([], port_range=3)
        assert result == []

    def test_get_possible_addresses_single_port(self):
        base = [Address("localhost", 5701)]
        
        result = AddressHelper.get_possible_addresses(base, port_range=1)
        
        assert len(result) == 1
        assert result[0] == Address("localhost", 5701)

    def test_get_possible_addresses_preserves_order(self):
        base = [Address("localhost", 5701)]
        
        result = AddressHelper.get_possible_addresses(base, port_range=3)
        
        assert result[0].port == 5701
        assert result[1].port == 5702
        assert result[2].port == 5703
