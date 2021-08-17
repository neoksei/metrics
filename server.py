import re
import asyncio


class ClientServerProtocol(asyncio.Protocol):
    _storage = dict()

    def connection_made(self, transport: asyncio.transports.Transport) -> None:
        self._transport = transport

    def data_received(self, data: bytes) -> None:
        response = self._process_data(data.decode())
        self._transport.write(response.encode())

    def _process_data(self, request: str) -> str:
        get_pattern = re.compile(r'get\s(\S+)\n')
        put_pattern = re.compile(r'put\s(\S+)\s(\d+(?:\.\d+)?)\s(\d+)\n')
        get_match = get_pattern.fullmatch(request)
        put_match = put_pattern.fullmatch(request)

        if get_match:
            metric = get_match.group(1)
            response = self._get(metric)

        elif put_match:
            metric = put_match.group(1)
            value = float(put_match.group(2))
            timestamp = int(put_match.group(3))
            response = self._put(metric, value, timestamp)

        else:
            response = 'error\nwrong command\n\n'

        return response

    def _get(self, metric: str) -> str:
        response = 'ok\n'

        if metric == '*':

            for key in self._storage:

                for timestamp, value in self._storage[key].items():
                    response += ' '.join([key, str(value), str(timestamp)]) + '\n'

        else:

            if metric in self._storage:

                for timestamp, value in self._storage[metric].items():
                    response += ' '.join([metric, str(value), str(timestamp)]) + '\n'

        return response + '\n'

    def _put(self, metric: str, value: float, timestamp: int) -> str:
        if metric in self._storage:
            self._storage[metric][timestamp] = value
        else:
            self._storage[metric] = {timestamp: value}

        return 'ok\n\n'


def run_server(host: str, port: int) -> None:
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    run_server('127.0.0.1', 8888)
