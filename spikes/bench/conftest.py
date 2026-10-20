import pytest
from pysail.spark import SparkConnectServer
from pyspark.sql.connect.session import SparkSession

@pytest.fixture(scope="session")
def spark():
    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    ip, port = server.listening_address
    session = SparkSession.builder.remote(f"sc://{ip}:{port}").getOrCreate()
    yield session
    session.stop()
    server.stop()
