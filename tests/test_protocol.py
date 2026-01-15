"""Unit tests for protocol layer and address handling."""

import struct
import unittest
from unittest.mock import patch, MagicMock

from hazelcast.protocol.client_message import (
    Frame,
    ClientMessage,
    BEGIN_FLAG,
    END_FLAG,
    UNFRAGMENTED_FLAG,
    IS_NULL_FLAG,
    BEGIN_DATA_STRUCTURE_FLAG,
    END_DATA_STRUCTURE_FLAG,
    IS_EVENT_FLAG,
    SIZE_OF_FRAME_LENGTH_AND_FLAGS,
    NULL_FRAME,
    BEGIN_FRAME,
    END_FRAME,
    TYPE_OFFSET,
    CORRELATION_ID_OFFSET,
    PARTITION_ID_OFFSET,
)
from hazelcast.network.address import Address, AddressHelper


class TestFrame(unittest.TestCase):
    """Tests for the Frame class."""

    def test_frame_creation_empty(self):
        frame = Frame()
        self.assertEqual(frame.content, b"")
        self.assertEqual(frame.flags, 0)
        self.assertEqual(len(frame), 0)

    def test_frame_creation_with_content(self):
        content = b"test data"
        frame = Frame(content, BEGIN_FLAG)
        self.assertEqual(frame.content, content)
        self.assertEqual(frame.flags, BEGIN_FLAG)
        self.assertEqual(len(frame), len(content))

    def test_frame_is_begin_frame(self):
        frame = Frame(b"", BEGIN_FLAG)
        self.assertTrue(frame.is_begin_frame)
        self.assertFalse(frame.is_end_frame)

    def test_frame_is_end_frame(self):
        frame = Frame(b"", END_FLAG)
        self.assertTrue(frame.is_end_frame)
        self.assertFalse(frame.is_begin_frame)

    def test_frame_is_null_frame(self):
        frame = Frame(b"", IS_NULL_FLAG)
        self.assertTrue(frame.is_null_frame)

    def test_frame_is_begin_data_structure_frame(self):
        frame = Frame(b"", BEGIN_DATA_STRUCTURE_FLAG)
        self.assertTrue(frame.is_begin_data_structure_frame)

    def test_frame_is_end_data_structure_frame(self):
        frame = Frame(b"", END_DATA_STRUCTURE_FLAG)
        self.assertTrue(frame.is_end_data_structure_frame)

    def test_frame_copy_with_new_flags(self):
        original = Frame(b"content", BEGIN_FLAG)
        copied = original.copy_with_new_flags(END_FLAG)
        self.assertEqual(copied.content, original.content)
        self.assertEqual(copied.flags, END_FLAG)
        self.assertEqual(original.flags, BEGIN_FLAG)

    def test_frame_equality(self):
        frame1 = Frame(b"test", BEGIN_FLAG)
        frame2 = Frame(b"test", BEGIN_FLAG)
        frame3 = Frame(b"test", END_FLAG)
        frame4 = Frame(b"other", BEGIN_FLAG)
        self.assertEqual(frame1, frame2)
        self.assertNotEqual(frame1, frame3)
        self.assertNotEqual(frame1, frame4)

    def test_frame_equality_with_non_frame(self):
        frame = Frame(b"test", 0)
        self.assertNotEqual(frame, "not a frame")
        self.assertNotEqual(frame, None)

    def test_predefined_frames(self):
        self.assertTrue(NULL_FRAME.is_null_frame)
        self.assertTrue(BEGIN_FRAME.is_begin_data_structure_frame)
        self.assertTrue(END_FRAME.is_end_data_structure_frame)


class TestClientMessage(unittest.TestCase):
    """Tests for the ClientMessage class."""

    def test_create_for_encode(self):
        msg = ClientMessage.create_for_encode()
        self.assertEqual(len(msg), 0)
        self.assertIsNone(msg.start_frame)

    def test_create_for_decode(self):
        frames = [Frame(b"test", UNFRAGMENTED_FLAG)]
        msg = ClientMessage.create_for_decode(frames)
        self.assertEqual(len(msg), 1)

    def test_add_frame(self):
        msg = ClientMessage()
        frame = Frame(b"test", 0)
        msg.add_frame(frame)
        self.assertEqual(len(msg), 1)
        self.assertEqual(msg.frames[0], frame)

    def test_start_frame(self):
        msg = ClientMessage()
        self.assertIsNone(msg.start_frame)
        frame = Frame(b"first", 0)
        msg.add_frame(frame)
        self.assertEqual(msg.start_frame, frame)

    def test_next_frame(self):
        frames = [Frame(b"first", 0), Frame(b"second", 0)]
        msg = ClientMessage(frames)
        self.assertEqual(msg.next_frame(), frames[0])
        self.assertEqual(msg.next_frame(), frames[1])
        self.assertIsNone(msg.next_frame())

    def test_peek_next_frame(self):
        frames = [Frame(b"first", 0)]
        msg = ClientMessage(frames)
        self.assertEqual(msg.peek_next_frame(), frames[0])
        self.assertEqual(msg.peek_next_frame(), frames[0])

    def test_has_next_frame(self):
        msg = ClientMessage([Frame(b"test", 0)])
        self.assertTrue(msg.has_next_frame())
        msg.next_frame()
        self.assertFalse(msg.has_next_frame())

    def test_reset_read_index(self):
        msg = ClientMessage([Frame(b"test", 0)])
        msg.next_frame()
        self.assertFalse(msg.has_next_frame())
        msg.reset_read_index()
        self.assertTrue(msg.has_next_frame())

    def test_skip_frame(self):
        frames = [Frame(b"first", 0), Frame(b"second", 0)]
        msg = ClientMessage(frames)
        msg.skip_frame()
        self.assertEqual(msg.next_frame(), frames[1])

    def test_message_type(self):
        content = bytearray(16)
        struct.pack_into("<I", content, TYPE_OFFSET, 0x12345)
        msg = ClientMessage([Frame(bytes(content), UNFRAGMENTED_FLAG)])
        self.assertEqual(msg.get_message_type(), 0x12345)

    def test_set_message_type(self):
        content = bytearray(16)
        msg = ClientMessage([Frame(bytes(content), UNFRAGMENTED_FLAG)])
        msg.set_message_type(0xABCDE)
        self.assertEqual(msg.get_message_type(), 0xABCDE)

    def test_correlation_id(self):
        content = bytearray(16)
        struct.pack_into("<q", content, CORRELATION_ID_OFFSET, 9876543210)
        msg = ClientMessage([Frame(bytes(content), UNFRAGMENTED_FLAG)])
        self.assertEqual(msg.get_correlation_id(), 9876543210)

    def test_set_correlation_id(self):
        content = bytearray(16)
        msg = ClientMessage([Frame(bytes(content), UNFRAGMENTED_FLAG)])
        msg.set_correlation_id(1234567890)
        self.assertEqual(msg.get_correlation_id(), 1234567890)

    def test_partition_id(self):
        content = bytearray(16)
        struct.pack_into("<i", content, PARTITION_ID_OFFSET, 42)
        msg = ClientMessage([Frame(bytes(content), UNFRAGMENTED_FLAG)])
        self.assertEqual(msg.get_partition_id(), 42)

    def test_set_partition_id(self):
        content = bytearray(16)
        msg = ClientMessage([Frame(bytes(content), UNFRAGMENTED_FLAG)])
        msg.set_partition_id(271)
        self.assertEqual(msg.get_partition_id(), 271)

    def test_get_message_type_empty(self):
        msg = ClientMessage()
        self.assertEqual(msg.get_message_type(), 0)

    def test_get_correlation_id_empty(self):
        msg = ClientMessage()
        self.assertEqual(msg.get_correlation_id(), 0)

    def test_get_partition_id_empty(self):
        msg = ClientMessage()
        self.assertEqual(msg.get_partition_id(), -1)

    def test_get_total_length(self):
        frame1 = Frame(b"test", 0)
        frame2 = Frame(b"data", 0)
        msg = ClientMessage([frame1, frame2])
        expected = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS + len(b"test") + len(b"data")
        self.assertEqual(msg.get_total_length(), expected)

    def test_to_bytes_and_from_bytes(self):
        content = bytearray(16)
        struct.pack_into("<I", content, TYPE_OFFSET, 0x100)
        struct.pack_into("<q", content, CORRELATION_ID_OFFSET, 42)
        struct.pack_into("<i", content, PARTITION_ID_OFFSET, 5)

        original = ClientMessage([Frame(bytes(content), 0)])
        serialized = original.to_bytes()
        restored = ClientMessage.from_bytes(serialized)

        self.assertEqual(restored.get_message_type(), 0x100)
        self.assertEqual(restored.get_correlation_id(), 42)
        self.assertEqual(restored.get_partition_id(), 5)

    def test_to_bytes_sets_begin_end_flags(self):
        msg = ClientMessage([Frame(b"only", 0)])
        data = msg.to_bytes()
        flags = struct.unpack_from("<H", data, 4)[0]
        self.assertTrue(flags & BEGIN_FLAG)
        self.assertTrue(flags & END_FLAG)

    def test_to_bytes_multiple_frames(self):
        msg = ClientMessage([Frame(b"first", 0), Frame(b"second", 0)])
        data = msg.to_bytes()

        offset = 0
        flags1 = struct.unpack_from("<H", data, offset + 4)[0]
        self.assertTrue(flags1 & BEGIN_FLAG)
        self.assertFalse(flags1 & END_FLAG)

        frame1_len = struct.unpack_from("<I", data, offset)[0]
        offset += frame1_len

        flags2 = struct.unpack_from("<H", data, offset + 4)[0]
        self.assertFalse(flags2 & BEGIN_FLAG)
        self.assertTrue(flags2 & END_FLAG)

    def test_from_bytes_incomplete_frame(self):
        data = struct.pack("<I", 100)
        msg = ClientMessage.from_bytes(data)
        self.assertEqual(len(msg), 0)

    def test_create_initial_frame(self):
        frame = ClientMessage.create_initial_frame(8, UNFRAGMENTED_FLAG)
        self.assertEqual(frame.flags, UNFRAGMENTED_FLAG)
        self.assertEqual(len(frame.content), 16 + 8)


class TestAddress(unittest.TestCase):
    """Tests for the Address class."""

    def test_address_creation(self):
        addr = Address("localhost", 5701)
        self.assertEqual(addr.host, "localhost")
        self.assertEqual(addr.port, 5701)

    def test_address_default_port(self):
        addr = Address("localhost")
        self.assertEqual(addr.port, Address.DEFAULT_PORT)

    def test_address_str(self):
        addr = Address("192.168.1.1", 5702)
        self.assertEqual(str(addr), "192.168.1.1:5702")

    def test_address_repr(self):
        addr = Address("myhost", 5703)
        self.assertEqual(repr(addr), "Address('myhost', 5703)")

    def test_address_equality(self):
        addr1 = Address("host1", 5701)
        addr2 = Address("host1", 5701)
        addr3 = Address("host2", 5701)
        addr4 = Address("host1", 5702)
        self.assertEqual(addr1, addr2)
        self.assertNotEqual(addr1, addr3)
        self.assertNotEqual(addr1, addr4)

    def test_address_equality_with_non_address(self):
        addr = Address("host", 5701)
        self.assertNotEqual(addr, "host:5701")
        self.assertNotEqual(addr, None)

    def test_address_hash(self):
        addr1 = Address("host", 5701)
        addr2 = Address("host", 5701)
        self.assertEqual(hash(addr1), hash(addr2))
        addresses = {addr1}
        self.assertIn(addr2, addresses)

    @patch("socket.getaddrinfo")
    def test_address_resolve(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("192.168.1.1", 5701)),
            (2, 1, 6, "", ("192.168.1.2", 5701)),
        ]
        addr = Address("myhost", 5701)
        resolved = addr.resolve()
        self.assertEqual(len(resolved), 2)
        self.assertIn(("192.168.1.1", 5701), resolved)
        self.assertIn(("192.168.1.2", 5701), resolved)

    @patch("socket.getaddrinfo")
    def test_address_resolve_caches_result(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("192.168.1.1", 5701)),
        ]
        addr = Address("myhost", 5701)
        addr.resolve()
        addr.resolve()
        mock_getaddrinfo.assert_called_once()

    @patch("socket.getaddrinfo")
    def test_address_resolve_deduplicates(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("192.168.1.1", 5701)),
            (2, 1, 6, "", ("192.168.1.1", 5701)),
        ]
        addr = Address("myhost", 5701)
        resolved = addr.resolve()
        self.assertEqual(len(resolved), 1)

    @patch("socket.getaddrinfo")
    def test_address_resolve_failure(self, mock_getaddrinfo):
        import socket
        mock_getaddrinfo.side_effect = socket.gaierror("DNS lookup failed")
        addr = Address("invalid.host", 5701)
        resolved = addr.resolve()
        self.assertEqual(resolved, [("invalid.host", 5701)])

    @patch("socket.getaddrinfo")
    def test_address_invalidate_cache(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("192.168.1.1", 5701)),
        ]
        addr = Address("myhost", 5701)
        addr.resolve()
        addr.invalidate_cache()
        addr.resolve()
        self.assertEqual(mock_getaddrinfo.call_count, 2)


class TestAddressHelper(unittest.TestCase):
    """Tests for the AddressHelper class."""

    def test_parse_host_only(self):
        addr = AddressHelper.parse("localhost")
        self.assertEqual(addr.host, "localhost")
        self.assertEqual(addr.port, Address.DEFAULT_PORT)

    def test_parse_host_and_port(self):
        addr = AddressHelper.parse("192.168.1.1:5702")
        self.assertEqual(addr.host, "192.168.1.1")
        self.assertEqual(addr.port, 5702)

    def test_parse_with_whitespace(self):
        addr = AddressHelper.parse("  localhost:5703  ")
        self.assertEqual(addr.host, "localhost")
        self.assertEqual(addr.port, 5703)

    def test_parse_ipv6_bracketed(self):
        addr = AddressHelper.parse("[::1]:5704")
        self.assertEqual(addr.host, "::1")
        self.assertEqual(addr.port, 5704)

    def test_parse_ipv6_bracketed_no_port(self):
        addr = AddressHelper.parse("[2001:db8::1]")
        self.assertEqual(addr.host, "2001:db8::1")
        self.assertEqual(addr.port, Address.DEFAULT_PORT)

    def test_parse_ipv6_unbracketed(self):
        addr = AddressHelper.parse("::1")
        self.assertEqual(addr.host, "::1")
        self.assertEqual(addr.port, Address.DEFAULT_PORT)

    def test_parse_invalid_port(self):
        addr = AddressHelper.parse("host:notaport")
        self.assertEqual(addr.host, "host:notaport")
        self.assertEqual(addr.port, Address.DEFAULT_PORT)

    def test_parse_list(self):
        addresses = AddressHelper.parse_list([
            "host1:5701",
            "host2:5702",
            "host3",
        ])
        self.assertEqual(len(addresses), 3)
        self.assertEqual(addresses[0], Address("host1", 5701))
        self.assertEqual(addresses[1], Address("host2", 5702))
        self.assertEqual(addresses[2], Address("host3", 5701))

    def test_parse_list_empty(self):
        addresses = AddressHelper.parse_list([])
        self.assertEqual(len(addresses), 0)

    def test_get_possible_addresses(self):
        base = [Address("host1", 5701)]
        expanded = AddressHelper.get_possible_addresses(base, port_range=3)
        self.assertEqual(len(expanded), 3)
        self.assertIn(Address("host1", 5701), expanded)
        self.assertIn(Address("host1", 5702), expanded)
        self.assertIn(Address("host1", 5703), expanded)

    def test_get_possible_addresses_deduplicates(self):
        base = [Address("host1", 5701), Address("host1", 5701)]
        expanded = AddressHelper.get_possible_addresses(base, port_range=2)
        self.assertEqual(len(expanded), 2)

    def test_get_possible_addresses_multiple_hosts(self):
        base = [Address("host1", 5701), Address("host2", 5701)]
        expanded = AddressHelper.get_possible_addresses(base, port_range=2)
        self.assertEqual(len(expanded), 4)


if __name__ == "__main__":
    unittest.main()
