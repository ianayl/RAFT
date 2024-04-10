from replica import Replica

if __name__ == "__main__":
    primary = Replica(50051, heartbeat_port=50053, primary_backups=["localhost:50052"])
    primary.start()
