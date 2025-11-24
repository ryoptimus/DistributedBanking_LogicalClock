import grpc
import banks_pb2
import banks_pb2_grpc

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None
        # set local clock
        self.local_clock = 0

    # Create the Customer stub
    def createStub(self):
        port = 5000 + self.id
        channel = grpc.insecure_channel(f'localhost:{port}')
        self.stub = banks_pb2_grpc.BankStub(channel)

    # Send out the events to the Bank
    def executeEvents(self):
        if self.stub is None:
            self.createStub()
        
        comment_str = f'event_sent from customer {self.id}'

        cust_msgs = {}
        cust_msgs[self.id] = []
        traces = []
        for event in self.events:
            self.local_clock += 1

            e_id = event['customer-request-id']
            e_in = event['interface']

            if event['interface'] == 'query':
                request = banks_pb2.MsgReq(type='customer', id=self.id, interface=e_in, money=0, event_id=e_id, logical_clock=self.local_clock, comment=comment_str)
                cust_msgs[self.id].append({'customer-request-id': request.event_id, 'logical_clock': request.logical_clock, 'interface': request.interface, 'comment': comment_str})
                response = self.stub.MsgDelivery(request)
                self.recvMsg.append(response)
                traces.extend(response.trace)

            if event['interface'] == 'deposit':
                request = banks_pb2.MsgReq(type='customer', id=self.id, interface=e_in, money=event['money'], event_id=e_id, logical_clock=self.local_clock, comment=comment_str)
                cust_msgs[self.id].append({'customer-request-id': request.event_id, 'logical_clock': request.logical_clock, 'interface': request.interface, 'comment': comment_str})
                response = self.stub.MsgDelivery(request)
                self.recvMsg.append(response)
                traces.extend(response.trace)
                
            if event['interface'] == 'withdraw':
                request = banks_pb2.MsgReq(type='customer', id=self.id, interface=e_in, money=event['money'], event_id=e_id, logical_clock=self.local_clock, comment=comment_str)
                cust_msgs[self.id].append({'customer-request-id': request.event_id, 'logical_clock': request.logical_clock, 'interface': request.interface, 'comment': comment_str})
                response = self.stub.MsgDelivery(request)
                self.recvMsg.append(response)
                traces.extend(response.trace)
        return cust_msgs, traces
            