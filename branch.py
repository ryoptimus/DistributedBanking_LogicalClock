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
        # a list of propagation messages between branches
        self.propMsg = list()

        self.server = None
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
            self.stubList.append(stub)

    def Deposit(self, request):
        comment_str = f'event_recv from {request.type} {self.id}'
        self.balance += request.money
        return banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result='success', event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)
    
    def Withdraw(self, request):
        comment_str = f'event_recv from {request.type} {self.id}'
        self.balance -= request.money
        return banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result='success', event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)

    def Propagate(self, request):
        if request.interface == 'deposit':
            return self.Propagate_Deposit(request)
        if request.interface == 'withdraw':
            return self.Propagate_Withdraw(request)
        return False

# TODO: Fix this
    def Propagate_Deposit(self, request):
        success = False
        
        for stub in self.stubList:
            comment_str = f'event_sent to branch _'
            request = banks_pb2.MsgReq(type='branch', id=self.id, interface='propagate_deposit', money=request.money, event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)
            self.propMsg.append(request)
            try:
                response = stub.MsgDelivery(request)
                self.propMsg.append(response)
                self.recvMsg.append(response)
                if response.result == 'success':
                    success = True
            except grpc.RpcError as e:
                print(f'RPC Error: {e}')
                success = False

        return success

    def Propagate_Withdraw(self, request):
        success = False

        for stub in self.stubList:
            comment_str = f'event_sent to branch {self.id}'
            request = banks_pb2.MsgReq(type='branch', id=self.id, interface='propagate_withdraw', money=request.money, event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)
            self.propMsg.append(request)
            try:
                response = stub.MsgDelivery(request)
                self.propMsg.append(response)
                self.recvMsg.append(response)
                success = True
            except grpc.RpcError as e:
                print(f'RPC Error: {e}')
                success = False
            
        return success
    
    def Query(self, request):
        comment_str = f'event_recv from {request.type} {self.id}'
        return banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result='success', event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)
    
    # Process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        comment_str = f'event_recv from {request.type} {self.id}'

        # Separate logic for customer and branch as customer requests necessitate propagation
        if request.type == 'customer':
            if request.interface == 'deposit':
                self.balance += request.money
                if self.Propagate(request):
                    status = 'success'
                else:
                    status = 'fail'
                return banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result=status, event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)
            if request.interface == 'withdraw':
                self.balance -= request.money
                if self.Propagate(request):
                    status = 'success'
                else:
                    status = 'fail'
                return banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result=status, event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)
            if request.interface == 'query':
                return self.Query(request)
        if request.type == 'branch':
            if request.interface == 'propagate_deposit':
                return self.Deposit(request)
            if request.interface == 'propagate_withdraw':
                return self.Withdraw(request)
        return banks_pb2.MsgReply(type='branch', id=self.id, interface=request.interface, balance=self.balance, result='fail', event_id=request.event_id, logical_clock=self.local_clock, comment=comment_str)