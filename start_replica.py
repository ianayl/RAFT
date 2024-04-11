import sys
import subprocess
from replica import Replica

import constants

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print(f"Please specify replica port! (within {constants.KNOWN_REPLICAS[0]} to {constants.KNOWN_REPLICAS[-1]})")
        print(f"Usage: python {sys.argv[0]} <replica port>")
        exit(1)

    if int(sys.argv[1]) in constants.KNOWN_REPLICAS:
        try:
            redis_server = subprocess.Popen(["redis-server", "--port", str(int(sys.argv[1])+100)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            primary = Replica(int(sys.argv[1]))
            primary.start()
        finally:
            redis_server.terminate()
    else:
        print(f"Please specify cluster number within {constants.KNOWN_REPLICAS[0]} to {constants.KNOWN_REPLICAS[-1]}!")
        exit(1)
