# CS 4459 - RAFT Election Algorithm
- A basic inventory management system using the RAFT election algorithm to ensure data consistency.

## Instructions
- Run `make` to generate any missing gRPC files from the provided proto files.
- To run the heartbeat service, run `python3 ./heartbeat_service.py`.
- To run the backup server, run `python3 ./backup.py`.
- To run the primary server, run `python3 ./primary.py`.
- To run the client, run `python3 ./client.py`.
- Exit the servers by pressing `Ctrl+C`. You may need to wait up to 5 seconds for the servers to exit.

## Notes
- The primary and backup will attempt to ping the heartbeat service every 5 seconds; even if the heartbeat service is not running.
- Failures to write will be printed to the console, but not logged.