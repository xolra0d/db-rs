import socket
import struct

def encode_command(command_parts):
    """Encode a command array into the custom protocol format"""
    result = bytearray([ord('*'), len(command_parts)])
    
    for part in command_parts:
        part_bytes = part.encode('utf-8')
        result.extend([ord('-'), len(part_bytes)])
        result.extend(part_bytes)
    
    return bytes(result)

def decode_command(data):
    """Decode response data from the custom protocol format"""
    if not data:
        return None
    
    def parse_unknown(buf, start_pos):
        """Parse unknown command type - either array (*) or string (-)"""
        if start_pos >= len(buf):
            raise ValueError("Unexpected end of data")
        
        marker = buf[start_pos]
        if marker == ord('*'):
            return parse_array(buf, start_pos + 1)
        elif marker == ord('-'):
            return parse_string(buf, start_pos + 1)
        elif marker == ord('!'):
            return parse_error(buf, start_pos + 1)
        else:
            raise ValueError(f"Unknown command type: {marker} ('{chr(marker)}')")
    
    def parse_simple(buf, start_pos):
        """Parse simple data - length byte followed by data"""
        if start_pos >= len(buf):
            raise ValueError("Unexpected end of data")
        
        char_count = buf[start_pos]
        if start_pos + 1 + char_count > len(buf):
            raise ValueError("Not enough data for string")
        
        data_bytes = buf[start_pos + 1:start_pos + 1 + char_count]
        return start_pos + 1 + char_count, data_bytes
    
    def parse_string(buf, start_pos):
        next_pos, data_bytes = parse_simple(buf, start_pos)
        return next_pos, data_bytes.decode('utf-8')

    def parse_error(buf, start_pos):
        next_pos, data_bytes = parse_simple(buf, start_pos)
        return next_pos, "Error: " + data_bytes.decode('utf-8')
    
    def parse_array(buf, start_pos):
        """Parse array command"""
        if start_pos >= len(buf):
            raise ValueError("Unexpected end of data")
        
        element_count = buf[start_pos]
        pos = start_pos + 1
        result = []
        
        for _ in range(element_count):
            pos, command = parse_unknown(buf, pos)
            result.append(command)
        
        return pos, result
    
    try:
        # Start parsing from the beginning
        _, result = parse_unknown(data, 0)
        
        # Convert lists to strings for better readability
        if isinstance(result, list):
            # Join list elements with spaces for better display
            return " ".join(str(item) for item in result)
        
        return result
    except Exception as e:
        print(f"Decode error: {e}")
        return None

def send_command(sock, command_parts):
    """Send a command to the server"""
    command_data = encode_command(command_parts)
    
    # Add header with message length
    header = struct.pack('<I', len(command_data))
    message = header + command_data
        
    sock.sendall(message)
    
    # Read response
    response_header = sock.recv(4)
    if len(response_header) != 4:
        print("Failed to read response header")
        return None
    
    response_length = struct.unpack('<I', response_header)[0]
    response_data = sock.recv(response_length)

    print(f"Response length: {len(response_data)} bytes")
    
    return decode_command(response_data)
        

def main():
    # Connect to server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(('127.0.0.1', 7070))
        print("Connected to server")

        while True:
            data = input("Command: ").split(" ")   
            if data == ["exit"]:
                break
            response = send_command(sock, data)
            
            # Format output based on response type
            if response is None:
                print("No response received")
            elif isinstance(response, bytes):
                print(f"Response (bytes): {response}")
            elif isinstance(response, str):
                if response.startswith("Error:"):
                    print(f"Error: {response[6:]}")  # Remove "Error: " prefix
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
