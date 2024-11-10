# GOOD VERSION
# All in one file
# Handles shutdown and connections
import xmlrpc.server
import xmlrpc.client
import threading
import time
import random
import sys
import socket 
import signal

# Configuration for 3 nodes with different ports
NODES = [("localhost", 8000), ("localhost", 8001), ("localhost", 8002)]
MAJORITY = len(NODES) // 2 + 1
INACTIVITY_TIMEOUT = 15  # Time in seconds after which the server will shut down if idle
socket.setdefaulttimeout(5)

shutdown_event = threading.Event()  # Global event to signal shutdown

class Node:
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.port = port
        self.file_value = None
        self.accepted_value = None
        self.accepted_id = 0
        self.proposed_id = 0
        self.last_activity = time.time()
        self.shutdown_flag = False
        self.consensus_reached = False
        self.simulation_case = 0
        
        # Initialize the server and threads
        self.rpc_server = xmlrpc.server.SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self.rpc_server.register_instance(self)
        self.rpc_server.logRequests = False  # Disable request logging
        self.rpc_server.allow_reuse_address = True


        self.server_thread = threading.Thread(target=self.rpc_server.serve_forever, daemon=True)
        self.server_thread.start()
        self.monitor_thread = threading.Thread(target=self.monitor_inactivity, daemon=True)
        self.monitor_thread.start()
        print(f"Node {node_id} started on port {port}")

    def submit_value(self, value, proposed_id=0, case=0):
        """Start the consensus process with a submitted value."""

        # Client cannot submit new values if consensus is already achieved
        if self.consensus_reached:
            print(f"[Node {self.node_id}] Cannot submit new value. Consensus already achieved.")
            return "Cannot submit new value. Consensus already achieved."

        self.simulation_case = case  # Used to help simulate different paxos conditions
        
        # Sets the proposed id (client can give an ID, or node chooses its own)
        if proposed_id == 0:
            self.proposed_id += random.randint(1, 5)
        else:
            self.proposed_id = proposed_id
        self.last_activity = time.time()
        print(f"[Node {self.node_id}] Received client submission: {value} with Proposal ID: {self.proposed_id}")
        
        # Initiate prepare phase
        result = self.prepare_phase(value)
        
        if result == "Success: Consensus reached on value.":
            print(f"[Node {self.node_id}] Consensus reached. Returning result to client.")
        else:
            print(f"[Node {self.node_id}] Could not reach consensus. Returning result to client.")
        
        return result
    
    def prepare_phase(self, value):
        """Prepare phase of Paxos. Ask for promises from other nodes and adopt any accepted value."""
        print(f"[Node {self.node_id}] Preparing with Proposal ID: {self.proposed_id}")
        promise_count = 1  # Count self-promise
        accepted_value = None
        highest_accepted_id = -1
        retries = 3

        if self.simulation_case == 1 and self.node_id == 1: 
            time.sleep(2)

        for attempt in range(retries):
            self.last_activity = time.time()
            for host, port in NODES:
                if port == self.port:
                    continue

                node_url = f"http://{host}:{port}"
                try:
                    with xmlrpc.client.ServerProxy(node_url) as node:
                        random_delay = random.uniform(1, 3)  # Random delay for network simulation
                        time.sleep(random_delay)
                        response = node.promise(self.proposed_id)
                        if response["promise"]:
                            promise_count += 1
                            # Adopt the value with the highest accepted ID
                            if response["accepted_value"] is not None and response["accepted_id"] > highest_accepted_id:
                                highest_accepted_id = response["accepted_id"]
                                accepted_value = response["accepted_value"]
                except socket.timeout:
                    print(f"[Node {self.node_id}] Timeout when connecting to Node at {host}:{port}")
                    continue
                except Exception as e:
                    print(f"[Node {self.node_id}] Error connecting to Node at {host}:{port}: {e}")
                    continue

            # If an accepted value was found, adopt it with the highest proposal ID
            if accepted_value is not None:
                print(f"[Node {self.node_id}] Adopting previously accepted value: {accepted_value} from proposal ID {highest_accepted_id}")
                value = accepted_value  # Adopt the accepted value
                self.accepted_id = highest_accepted_id  # Update the proposal ID of the adopted value

            # Proceed to accept phase if we have a majority of promises
            if promise_count >= MAJORITY:
                return self.accept_phase(value)
            else:
                # Backoff and retry prepare phase if majority promises are not reached
                print(f"[Node {self.node_id}] Failed to gather majority promises.")
                time.sleep(random.uniform(1, 3))
                self.proposed_id += random.randint(1, 5)
                
        return "Failure: Could not reach consensus."


    def promise(self, proposal_id):
        """Respond to prepare requests with a promise if the proposal ID is higher."""
        self.last_activity = time.time()
        # Grant promise if no accepted_id or if proposal_id is higher than both accepted_id and proposed_id
        if (self.accepted_id == 0 or proposal_id > self.accepted_id) and (proposal_id > self.proposed_id):
            response = {
                "promise": True,
                "accepted_value": self.accepted_value,
                "accepted_id": self.accepted_id  # Ensure accepted_id is included
            }
            self.accepted_id = proposal_id
            print(f"[Node {self.node_id}] Promised Proposal ID: {proposal_id}")
            return response
        else:
            promised_id = self.accepted_id or self.proposed_id
            print(f"[Node {self.node_id}] Rejected Proposal ID: {proposal_id}, already promised: {promised_id}")
            return {"promise": False, "accepted_value": None, "accepted_id": None}

    def accept_phase(self, value):
        """Accept phase of Paxos. Attempt to gain acceptance for the proposal."""
        if self.consensus_reached:
            return "Failure: Consensus reached on a previous value."

        if self.simulation_case == 3:
            time.sleep(random.uniform(5, 10))  # Random delay for livelock simulation

        print(f"[Node {self.node_id}] Proposing acceptance of value: {value} with Proposal ID: {self.proposed_id}")
        
        # Abort if proposal ID is outdated
        if self.proposed_id < self.accepted_id:
            print(f"[Node {self.node_id}] Cannot propose acceptance for Proposal ID: {self.proposed_id} "
                f"as it is lower than the promised Proposal ID: {self.accepted_id}. Aborting.")
            return "Failure: Proposal ID is outdated due to a higher promise."
       
        if self.simulation_case == 2 and self.node_id == 0:
            time.sleep(2)
      
        accept_count = 0
        for host, port in NODES:
            self.last_activity = time.time()
            if port == self.port:
                if self.accept(value, self.proposed_id):
                    accept_count += 1  # Count itself as accepted
                if accept_count >= MAJORITY:
                    break
                continue 
            node_url = f"http://{host}:{port}"

            if self.simulation_case == 1 and self.node_id == 0:
                time.sleep(5)
            try:  # Send acceptance request to all other nodes
                with xmlrpc.client.ServerProxy(node_url) as node:
                    accepted = node.accept(value, self.accepted_id)
                    if accepted:
                        accept_count += 1
            except Exception as e:
                print(f"[Node {self.node_id}] Error connecting to Node at {host}:{port}: {e}")

        if accept_count >= MAJORITY:  # Consensus Achieved
            print(f"[Node {self.node_id}] Consensus reached on value: {value}")
            self.broadcast_consensus_value(value)  # Broadcast consensus value to all other nodes
            return "Success: Consensus reached on value."
        else:  # Consensus Failed
            print(f"[Node {self.node_id}] Failed to reach consensus.")
            return "Failure: Could not reach consensus."


    def accept(self, value, accepted_id):
        """Respond to acceptance requests with the proposed value."""
        self.last_activity = time.time()
        if self.accepted_value is None or accepted_id > self.accepted_id:
            self.accepted_value = value
            self.accepted_id = accepted_id
            print(f"[Node {self.node_id}] Accepted value: {value}")
            return True
        else:
            print(f"[Node {self.node_id}] Rejected value: {value}, already accepted: {self.accepted_value}")
            return False

    def broadcast_consensus_value(self, value):
        """Broadcast the consensus value to all nodes for consistency."""
        for host, port in NODES: 
            if port == self.port:
                self.update_cisc5597(value)
                continue
            node_url = f"http://{host}:{port}"
            try:
                with xmlrpc.client.ServerProxy(node_url) as node:
                    node.update_cisc5597(value)  # Update file for each node
                print(f"[Node {self.node_id}] Broadcasted consensus value: {value} to Node at {host}:{port}")
            except Exception as e:
                print(f"[Node {self.node_id}] Error broadcasting to Node at {host}:{port}: {e}")

    def update_cisc5597(self, value):
        """Write the consensus value to a local file to persist the decision."""
        self.consensus_reached = True
        self.file_value = value
        file_path = f"CISC5597_node_{self.node_id}.txt"
        with open(file_path, 'w') as f:
            f.write(value)
        print(f"[Node {self.node_id}] Written to file '{file_path}' with content: {value}")

    def monitor_inactivity(self):
        """Monitor for inactivity and shut down if no activity within timeout."""
        while not self.shutdown_flag and not shutdown_event.is_set():
            if time.time() - self.last_activity >= INACTIVITY_TIMEOUT:
                print(f"[Node {self.node_id}] No activity detected for {INACTIVITY_TIMEOUT} seconds. Shutting down.")
                shutdown_event.set()  # Signal to main thread that shutdown is complete
                self.shutdown()
                break
            time.sleep(1)

    def shutdown(self):
        """Gracefully shut down the node's server and thread."""
        if not self.shutdown_flag:
            self.shutdown_flag = True
            print(f"[Node {self.node_id}] Shutting down.")
            self.rpc_server.shutdown()
            self.server_thread.join()  # Wait for server thread to terminate

    def run(self):
        """Run the node's RPC server."""
        try:
            self.rpc_server.serve_forever()
        except Exception as e:
            print(f"[Node {self.node_id}] Server encountered an error: {e}")
            
def start_node(node_id, port):
    """Initialize and start a node on a separate thread."""
    node = Node(node_id, port)
    node.run()

# Unified shutdown handler for Ctrl+C
def signal_handler(sig, frame):
    print("\nShutdown signal received. Stopping all nodes...")
    shutdown_event.set()  # Signal all threads to stop

if __name__ == "__main__":
    # Register the signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    threads = []
    for i, (host, port) in enumerate(NODES):
        t = threading.Thread(target=start_node, args=(i, port), daemon=True)
        t.start()
        threads.append(t)

    # Wait for the shutdown event
    while not shutdown_event.is_set():
        time.sleep(1)

    print("All nodes have shut down.")
    sys.exit(0)
