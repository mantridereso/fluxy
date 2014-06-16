__author__ = 'martin'

import zmq
import json
import threading

from agent.Agent import ParticularAgent, AgentAddress, ALLGroup, AbstractAgentIdentity
from message.Messages import AbstractMessage
from MTL import *

from Config import Config


class ZMQAgentAddress(MTLAgentAddress):
    def __init__(self, zmqAddress):
        MTLAgentAddress.__init__(self)
        self._zmqAddress = zmqAddress

    def export(self):
        return self._zmqAddress

    def getZMQAddress(self):
        return self._zmqAddress


class ZMQAgent(MTLAgent):

    def getAddress(self):
        return ZMQAgentAddress(self._zmqAddress)

    def start(self):
        pass

    class Receiver(threading.Thread):

        tSum = 0
        cntMessages = 0

        def __init__(self, socket, agent):
            threading.Thread.__init__(self)
            self._socket = socket
            self._agent = agent

        def run(self):

            while True:
                stringMessage = self._socket.recv()
                self._agent.getFluxyAgent().enqueueMessage(stringMessage)

        @staticmethod
        def getTSum():
            return ZMQAgent.Receiver.tSum

    def __init__(self, socket, zmqAddress):
        MTLAgent.__init__(self)
        self._zmqAddress = zmqAddress
        self._ctx = zmq.Context()
        receiverThread = self.Receiver(socket, self)
        receiverThread.start()
        self._sockets = dict()

    def sendMessage(self, message):
        isMessageObject = isinstance(message, AbstractMessage)
        if isMessageObject:
            address = message.getReceiver().getAgent().getAddress().getMTLAddress().getZMQAddress()
        else:
            address = message[AbstractMessage.KEY_HEADER][AbstractMessage.AbstractHeader.KEY_RECEIVER][AbstractAgentIdentity.KEY_ACTOR]
        if not address in self._sockets:
            s = self._ctx.socket(zmq.PUSH)
            s.setsockopt(zmq.SNDBUF, 32768)
            s.setsockopt(zmq.SNDHWM, 32768)
            s.connect(address)
            self._sockets[address] = s
        self._sockets[address].send(json.dumps(message.export() if isMessageObject else message))


class ZMQ_MTL(MTL):

    def __init__(self):
        self._ctx = zmq.Context()
            
    def getPlatformManager(self):
        return ParticularAgent(AgentAddress(ZMQAgentAddress("tcp://"
                                                            + Config.ZMQ.PLATFORM_ADDRESS + ":"
                                                            + Config.ZMQ.PLATFORM_MANAGER_PORT)))\
            .inRole(ALLGroup.ROLE_MANAGER)\
            .withinGroup(ALLGroup.NAME)

    def getMTLAgent(self, agentName=None):
        for _from, _to in Config.ZMQ.PORT_RANGES:
            socket = self._ctx.socket(zmq.PULL)
            socket.setsockopt(zmq.RCVBUF, 32768)
            socket.setsockopt(zmq.RCVHWM, 32768)
            try:
                port = socket.bind_to_random_port("tcp://*", _from, _to, _to - _from)
                zmqAddress = "tcp://"+Config.ZMQ.LOCAL_ADDRESS+":"+str(port)
                return ZMQAgent(socket, zmqAddress)
            except zmq.ZMQBindError as e:
                print str(e)
                pass

        raise BindException()

    def getPlatformManagerMTLAgent(self):
        socket = self._ctx.socket(zmq.PULL)
        socket.setsockopt(zmq.RCVBUF, 32768)
        socket.setsockopt(zmq.RCVHWM, 32768)
        try:
            socket.bind("tcp://*:"+Config.ZMQ.PLATFORM_MANAGER_PORT)
            zmqAddress = "tcp://"+Config.ZMQ.LOCAL_ADDRESS+":"+Config.ZMQ.PLATFORM_MANAGER_PORT
            return ZMQAgent(socket, zmqAddress)
        except zmq.ZMQBindError:
            raise BindException()


    def parseMTLAddress(self, mtlAddressRaw):
        return ZMQAgentAddress(mtlAddressRaw)


class BindException(Exception):
    pass
