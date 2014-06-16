__author__ = 'martin'


class MTLAgentAddress():

    def __init__(self):
        pass

    def export(self):
        raise NotImplemented


class MTLAgent():

    def __init__(self):
        self.fluxyAgent = None

    def bindToFluxyAgent(self, fluxyAgent):
        from agent.Agent import Agent
        assert isinstance(fluxyAgent, Agent)
        self.fluxyAgent = fluxyAgent

    def sendMessage(self, message):
        """

        :param message: influx.Message
        :raise NotImplemented:
        """
        raise NotImplemented

    def getAddress(self):
        """


        :raise NotImplemented:
        """
        raise NotImplemented

    def start(self):
        """


        :raise NotImplemented:
        """
        raise NotImplemented

    def receiveMessage(self, message):
        if self.fluxyAgent:
            self.fluxyAgent.receiveMessage(message)

    def getFluxyAgent(self):

        """

        :rtype : Agent
        """
        return self.fluxyAgent


class NoMTLLayerBinding(Exception):
    pass

class NotAnMTLAgent(Exception):
    pass


class MTL:

    instanceMTL = None

    def getMTLAgent(self, agentName=None):
        """

        :rtype : MTLAgent
        """
        raise NotImplemented
    
    def getPlatformManager(self):
        """

        :rtype : AbstractAgentIdentity
        """
        raise NotImplemented
    
    def getPlatformManagerMTLAgent(self):
        """

        :rtype : MTLAgent
        """
        raise NotImplemented
    
    def parseMTLAddress(self, mtlAddressRaw):
        """

        :rtype : MTLAgentAddress
        """
        raise NotImplemented

def getMTLImplementation():
    """

    :rtype : MTL
    """
    if MTL.instanceMTL is None:
        from zmq_mtl.FluxyZMQ import ZMQ_MTL
        MTL.instanceMTL = ZMQ_MTL()

    return MTL.instanceMTL