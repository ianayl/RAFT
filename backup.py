from replica import Replica

if __name__ == "__main__":
    backup = Replica(50052, heartbeat_port=50053)
    backup.start()
