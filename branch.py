import json
import threading

import grpc
import banks_pb2
import banks_pb2_grpc

# TODO: Figure out how to write to output.json from here

class Branch(banks_pb2_grpc.BankServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()

        self.server = None
        # a list of propagate messages
        self.propMsg = list()
        # store events in memory
        self.events = list()
        # set local clock
        self.local_clock = 0

    # Set server after starting
    def set_server(self, server):
        self.server = server

    # Stop server method
    def stop_server(self):
        if self.server:
            self.server.stop(1.0)
            print(f'Branch ID {self.id} stopped')

    def Shutdown(self, request, context):
        threading.Thread(target=self.stop_server, daemon=True).start()
        return banks_pb2.ShutdownReply(status='success')

    def populateStubList(self):
        for id in self.branches:
            if id == self.id:
                continue
            port = 5000 + id
            channel = grpc.insecure_channel(f'localhost:{port}')
            stub = banks_pb2_grpc.BankStub(channel)
            self.stubList.append((id, stub))
    
    def tick(self, remote_clock=None):
        if remote_clock == None:
            self.local_clock += 1
        else:
            self.local_clock = max(self.local_clock, remote_clock) + 1
        return self.local_clock

    def logEvent(self, event_id, interface, comment):
        event = banks_pb2.Event(
            id=self.id,
            event_id=event_id,
            logical_clock=self.local_clock,
            interface=interface,
            comment=comment
        )

        self.events.append(event)
        return event

    def Deposit(self, request):
        self.tick(request.logical_clock)

        trace = []
        comment_str = f'event_recv from {request.type} {request.id}'
        self.balance += request.money
        event = self.logEvent(request.event_id, request.interface, comment_str)
        trace.append(event)
        reply = banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result='success', event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str, trace=trace)
        self.recvMsg.append(reply)
        return reply
    
    def Withdraw(self, request):
        self.tick(request.logical_clock)

        trace = []
        comment_str = f'event_recv from {request.type} {request.id}'
        self.balance -= request.money
        event = self.logEvent(request.event_id, request.interface, comment_str)
        trace.append(event)
        reply = banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result='success', event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str, trace=trace)
        self.recvMsg.append(reply)
        return reply

    def Propagate(self, request):
        if request.interface == 'deposit':
            return self.Propagate_Deposit(request)
        if request.interface == 'withdraw':
            return self.Propagate_Withdraw(request)
        return [], False

    def Propagate_Deposit(self, request):
        success = False
        trace = []
        
        for branch_id, stub in self.stubList:
            if branch_id == self.id:
                continue

            self.tick()

            comment_str = f'event_sent to branch {branch_id}'
            event = self.logEvent(request.event_id, 'propagate_deposit', comment_str)
            trace.append(event)
            prop_req = banks_pb2.MsgReq(type='branch', id=self.id, interface='propagate_deposit', money=request.money, event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)
            self.propMsg.append(prop_req)
            try:
                response = stub.MsgDelivery(prop_req)
                self.propMsg.append(response)
                trace.extend(response.trace)
                # self.recvMsg.append(response)
                if response.result == 'success':
                    success = True
            except grpc.RpcError as e:
                print(f'RPC Error: {e}')
                success = False

        return trace, success

    def Propagate_Withdraw(self, request):
        success = False
        trace = []

        for branch_id, stub in self.stubList:
            if branch_id == self.id:
                continue

            self.tick()

            comment_str = f'event_sent to branch {branch_id}'
            event = self.logEvent(request.event_id, 'propagate_withdraw', comment_str)
            trace.append(event)
            prop_req = banks_pb2.MsgReq(type='branch', id=self.id, interface='propagate_withdraw', money=request.money, event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)
            self.propMsg.append(prop_req)
            try:
                response = stub.MsgDelivery(prop_req)
                self.propMsg.append(response)
                trace.extend(response.trace)
                # self.recvMsg.append(response)
                success = True
            except grpc.RpcError as e:
                print(f'RPC Error: {e}')
                success = False
            
        return trace, success
    
    def Query(self, request):
        self.tick(request.logical_clock)

        trace = []
        comment_str = f'event_recv from {request.type} {request.id}'
        event = self.logEvent(request.event_id, request.interface, comment_str)
        trace.append(event)
        reply = banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result='success', event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str, trace=trace)
        self.recvMsg.append(reply)
        return reply
    
    # Process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        comment_str = f'event_recv from {request.type} {request.id}'
        trace = []

        # Separate logic for customer and branch as customer requests necessitate propagation
        if request.type == 'customer':
            self.tick(request.logical_clock)

            event = self.logEvent(request.event_id, request.interface, comment_str)
            trace.append(event)
            if request.interface == 'deposit':
                self.balance += request.money
                prop_trace, success = self.Propagate(request)
                if success:
                    status = 'success'
                else:
                    status = 'fail'
                trace.extend(prop_trace)
                reply = banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result=status, event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str, trace=trace)
                self.recvMsg.append(reply)
                return reply
            if request.interface == 'withdraw':
                self.balance -= request.money
                prop_trace, success = self.Propagate(request)
                if success:
                    status = 'success'
                else:
                    status = 'fail'
                trace.extend(prop_trace)
                reply = banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result=status, event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str, trace=trace)
                self.recvMsg.append(reply)
                return reply
            if request.interface == 'query':
                return self.Query(request)
        if request.type == 'branch':
            if request.interface == 'propagate_deposit':
                return self.Deposit(request)
            if request.interface == 'propagate_withdraw':
                return self.Withdraw(request)
        return banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result='fail', event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str, trace=trace)