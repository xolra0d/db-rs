import socket
import struct
import msgpack

def encode_command(command_parts):
    """Encode a command array using MessagePack to match Rust Command enum"""
    command_array = [{"String": part} for part in command_parts]
    return msgpack.packb({"Array": command_array})

def decode_command(data):
    """Decode response data using MessagePack"""
    if not data:
        return None
    
    try:
        result = msgpack.unpackb(data, raw=False)
        
        if isinstance(result, dict):
            if 'String' in result:
                return result['String']
            elif 'Array' in result:
                array_items = []
                for item in result['Array']:
                    if isinstance(item, dict) and 'String' in item:
                        array_items.append(item['String'])
                    else:
                        array_items.append(str(item))
                return " ".join(array_items)
            elif 'error' in result:
                return f"Error: {result['error']}"
        
        if isinstance(result, list):
            return " ".join(str(item) for item in result)
        
        return str(result)
    except Exception as e:
        print(f"Decode error: {e}")
        return None

def send_command(sock, command_parts):
    """Send a command to the server"""
    command_data = encode_command(command_parts)
    
    header = struct.pack('<I', len(command_data))
    message = header + command_data
        
    sock.sendall(message)
    
    response_header = sock.recv(4)
    if len(response_header) != 4:
        print("Failed to read response header")
        return None
    
    response_length = struct.unpack('<I', response_header)[0]
    response_data = sock.recv(response_length)

    print(f"Response length: {len(response_data)} bytes")
    
    return decode_command(response_data)
        

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(('127.0.0.1', 7070))
        print("Connected to server")

        while True:
            data = input("Command: ").split(" ")   
            if data == ["exit"]:
                break
            response = send_command(sock, data)
            
            if response is None:
                print("No response received")
            elif isinstance(response, str):
                if response.startswith("Error:"):
                    print(f"Error: {response[6:]}")
                else:
                    print(f"Response: {response}")
            else:
                print(f"Response: {response}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        sock.close()
        print("Connection closed")

if __name__ == "__main__":
    main()
