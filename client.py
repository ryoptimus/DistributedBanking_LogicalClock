import os
import sys
import time
import json

import grpc
import banks_pb2
import banks_pb2_grpc

from customer import Customer

def run():
    if len(sys.argv) != 2:
        print('Error. Must provide the name of one JSON file in the project directory as an argument.')
        return
    
    filename = sys.argv[-1]
    if '.json' not in filename:
        print('Error. Argument must be a JSON file.')
        return
    
    # Write over output file if exists
    if os.path.exists('output.json'):
        open('output.json', 'w').close()
    
    # Open and read input.json
    with open(filename) as input_file:
        inputs = json.loads(input_file.read())

    customer_entries = []
    branch_ids = []

    i = 1
    for input in inputs:
        if input['type'] == 'customer':
            customer_entries.append(input)
        if input['type'] == 'branch':
            branch_ids.append(input['id'])
        i += 1


    all_cust_txns = []
    all_traces = []
    for ce in customer_entries:
        customer = Customer(
            id = ce['id'],
            events = ce['events']
        )
        customer.createStub()
        cust_txns, traces = customer.executeEvents()
        all_cust_txns.append(cust_txns)
        all_traces.extend(traces)

        time.sleep(1)

    time.sleep(3)

    # print(f'Branch transactions: {all_cust_txns}\n')

    merged_cust_txns = {}

    for c_txn in all_cust_txns:
        for key, value in c_txn.items():
            # print(f'Key: {key}, value: {value}')
            if merged_cust_txns.get(key) == None:
                merged_cust_txns[key] = []
            for v in value:
                merged_cust_txns[key].append(v)

    # print(f'Merged branch transactions: {merged_branch_txns}\n')

    part_one = []
    for c_txn in merged_cust_txns:
        # print(f'Branch: {c_txn}, Transactions: {merged_cust_txns[c_txn]}')
        c_id = c_txn
        txns = merged_cust_txns[c_id]
        part_one.append({'id': c_id, 'type': 'customer', 'events': txns})

    print(f'\nAll traces:\n{all_traces}')
    branch_events = {}
    for trace in all_traces:
        b_id = trace.id
        if branch_events.get(b_id) == None:
            branch_events[b_id] = []
        branch_events[b_id].append({
                'customer-request-id': trace.event_id,
                'logical_clock': trace.logical_clock,
                'interface': trace.interface,
                'comment': trace.comment
        })

    part_two = []
    for b_ev in branch_events:
        b_id = b_ev
        evs = branch_events[b_id]
        part_two.append({'id': b_id, 'type': 'branch', 'events': evs})

    # Write output file
    with open('output.json', 'w') as file:
        json.dump(part_one, file, indent=2)
        file.write('\n\n')
        json.dump(part_two, file, indent=2)

    for b_id in branch_ids:
        port = 5000 + b_id
        channel = grpc.insecure_channel(f'localhost:{port}')
        stub = banks_pb2_grpc.BankStub(channel)
        try:
            response = stub.Shutdown(banks_pb2.ShutdownReq(msg='init'), timeout=2.0)
            print(f'Shutdown of branch ID {b_id}: {response.status}')
        except grpc.RpcError as e:
            print(f'Shutdown of branch ID {b_id} failed:\n{e}')

if __name__ == '__main__':
    run()
