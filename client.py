import socket
import time


class ClientError(Exception):
    pass


class Client:
    """
    This is a client class for a metrics server.
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 8888, timeout: int = None) -> None:
        """
        Initialize client object with ip address, port number and connection timeout.

        :param host: ip address of server.
        :param port: server port.
        :param timeout: timeout for connection.
        """
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(timeout)
            self._socket.connect((host, port))
        except socket.error as err:
            raise ClientError('Cannot create connection', err)

    def get(self, request: str) -> dict:
        """
        Get data from the server.

        :param request: either a metric or an asterisk.
        :return: if an asterisk was passed then returns all metrics,
                 else returns specific metric in format {metric:
                 [(timestamp, value), ...]}, where values
                 sorted by timestamp
        """
        self._socket.sendall(f'get {request}\n'.encode())
        data = bytes()

        while True:
            try:
                data += self._socket.recv(4096)
            except socket.error as err:
                raise ClientError('Error reading data from socket', err)

            # '\n\n' signals the end of the message
            if data.endswith(b'\n\n'):
                break

        data = data.decode()[:-2].split('\n')

        if data[0] != 'ok':
            raise ClientError('Server returns an error')
        else:
            try:
                metrics = dict()

                for m, v, t in list(map(str.split, data[1:])):

                    if m in metrics:
                        metrics[m].append((int(t), float(v)))
                    else:
                        metrics[m] = [(int(t), float(v))]

                for m in metrics:
                    metrics[m] = sorted(metrics[m])

            except ValueError:
                raise ClientError('Server returns invalid data')

        return metrics

    def put(self, metric: str, value: float, timestamp: int = None) -> None:
        """
        Send data to the server.

        :param metric: metric, which you want do update.
        :param value: the value of the metric.
        :param timestamp: measurement time. Default is time.time().
        """
        if timestamp is None:
            timestamp = int(time.time())

        try:
            self._socket.sendall(f'put {metric} {value} {timestamp}\n'.encode())
        except socket.error as err:
            raise ClientError('Error sending data to server', err)
        try:
            data = self._socket.recv(1024)
        except socket.error as err:
            raise ClientError('Error reading data from socket', err)

        data = data.decode()

        if data != 'ok\n\n':
            raise ClientError('Server returns an error')

    def close(self) -> None:
        try:
            self._socket.close()
        except socket.error as err:
            raise ClientError("Error occurs while closing connection", err)
