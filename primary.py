from replica import Replica

if __name__ == "__main__":
    primary = Replica(50051)
    primary.start()
