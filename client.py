import argparse
import requests
import sys

def main():
    parser = argparse.ArgumentParser(description="CLI для работы с распределённым хранилищем")

    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')

    add_parser = subparsers.add_parser('add', help='Добавить ключ-значение')
    add_parser.add_argument('key', help='Ключ')
    add_parser.add_argument('value', help='Значение')

    get_parser = subparsers.add_parser('get', help='Получить значение по ключу')
    get_parser.add_argument('key', help='Ключ')

    delete_parser = subparsers.add_parser('delete', help='Удалить ключ')
    delete_parser.add_argument('key', help='Ключ')
    delete_parser.add_argument('value', help='Значение')

    all_values_parser = subparsers.add_parser('all_values', help='Посмотреть все значения')

    # Опция для указания базового URL главного сервера
    parser.add_argument('--base-url', default='http://localhost:8000', help='Базовый URL главного сервера')

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    base_url = args.base_url

    try:
        if args.command == 'add':
            data = {"key": args.key, "value": args.value}
            r = requests.post(f"{base_url}/add", json=data)
            r.raise_for_status()
            print(r.json())

        elif args.command == 'get':
            r = requests.get(f"{base_url}/get", params={"key": args.key})
            r.raise_for_status()
            print(r.json())

        elif args.command == 'delete':
            data = {"key": args.key, "value": args.value}
            r = requests.post(f"{base_url}/delete", json=data)
            r.raise_for_status()
            print(r.json())

        # elif args.command == 'add_node':
        #     data = {"node_key": args.node_key, "node_url": args.node_url}
        #     r = requests.post(f"{base_url}/add_node", json=data)
        #     r.raise_for_status()
        #     print(r.json())
        #
        # elif args.command == 'remove_node':
        #     data = {"node_key": args.node_key}
        #     r = requests.post(f"{base_url}/remove_node", json=data)
        #     r.raise_for_status()
        #     print(r.json())

        elif args.command == 'all_values':
            r = requests.get(f"{base_url}/all_values")
            r.raise_for_status()
            print(r.json())


    except requests.RequestException as e:
        print("Ошибка при выполнении запроса:", e)
        sys.exit(1)

if __name__ == '__main__':
    main()
