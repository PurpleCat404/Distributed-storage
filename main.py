import sys
from nodes_helper import NodesHelper

def main():
    print("Welcome to the Distributed Key-Value Store!")
    print("Available commands:")
    print("  add <key> <value>       - Add a key-value pair")
    print("  get <key>               - Get the value for a key")
    print("  delete <key> <value>    - Delete a key-value pair")
    print("  exit                    - Exit the program")

    nodes = NodesHelper()

    while True:
        try:
            command = input("Enter command: ").strip()
            if not command:
                continue

            parts = command.split(" ", 2)
            cmd = parts[0].lower()

            if cmd == "add":
                if len(parts) < 3:
                    print("Usage: add <key> <value>")
                    continue
                key, value = parts[1], parts[2]
                nodes.add_element(key, value)

            elif cmd == "get":
                if len(parts) < 2:
                    print("Usage: get <key>")
                    continue
                key = parts[1]
                value = nodes.get_value(key)
                if value is not None:
                    print(f"Value for '{key}': {value}")
                else:
                    print(f"Key '{key}' not found.")

            elif cmd == "delete":
                if len(parts) < 3:
                    print("Usage: delete <key> <value>")
                    continue
                key, value = parts[1], parts[2]
                nodes.delete_element(key, value)

            elif cmd == "exit":
                print("Goodbye!")
                break

            else:
                print("Unknown command. Type 'exit' to quit or see available commands above.")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            sys.exit(0)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
