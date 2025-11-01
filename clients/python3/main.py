import socket
import struct
import argparse
import msgpack
import prettytable


def encode_message(message: str) -> bytes:
    message_bytes = message.encode("utf-8")
    message_length = len(message_bytes)

    length_prefix = struct.pack("<Q", message_length)
    return length_prefix + message_bytes


def decode_and_print_table(message_bytes: bytes):
    message: dict = msgpack.unpackb(message_bytes)

    if columns := message.get("Ok"):
        table = prettytable.PrettyTable()
        for column in columns[0]:
            column_name = column[0][0]
            data = []
            for val in column[1]:
                data.append(list(val.values())[0])
            table.add_column(column_name, data)
        print(table)
    elif error := message.get("Err"):
        print(f"Error: {error}")

def run(host: str, port: int):
    try:
        print(f"Connecting to {host}:{port}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print("Connected to TouchHouse server")

        while True:
            sql_command = input("> ")

            encoded_command = encode_message(sql_command)
            _ = sock.send(encoded_command)


            header_bytes = sock.recv(8)
            if not header_bytes:
                print("Connection ended.")
                return
            response_length = struct.unpack("<Q", header_bytes)[0]
            message_bytes = sock.recv(response_length)

            print()
            decode_and_print_table(message_bytes)
            print()

    except ConnectionRefusedError:
        print(f"Couldn't connect to {host}:{port}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("host", help="Server host address")
    _ = parser.add_argument("port", type=int, help="Server port number")

    args = parser.parse_args()
    run(args.host, args.port)
