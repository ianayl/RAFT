from replica import Replica

if __name__ == "__main__":
    backup = Replica(50052)
    backup.start()
