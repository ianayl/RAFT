# CS 4459 - RAFT Election Algorithm
A basic inventory management system using the RAFT election algorithm to ensure data consistency.

## Instructions
- Run `make` to generate any missing gRPC files from the provided proto files.
- By default, there are 3 servers to chose from. Please modify `./constants.py` for more.
- To run a server, run `python3 ./start_replica.py <port number>` -- A Redis database be started automatically.
- Exit the servers by pressing `Ctrl+C`. You may need to wait up to 5 seconds for the servers to exit.

## Notes
- The primary and backup will attempt to ping the heartbeat service every 5 seconds; even if the heartbeat service is not running.
- Failures to write will be printed to the console, but not logged.
