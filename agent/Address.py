__author__ = 'martin'

from MTL import MTLAgentAddress, getMTLImplementation

class AgentAddress():

    def __init__(self, MTLAddress):
        assert isinstance(MTLAddress, MTLAgentAddress)
        self.MTLAddress = MTLAddress

    def getMTLAddress(self):
        return self.MTLAddress

    def export(self):
        return self.getMTLAddress().export()

    def exportId(self):
        return self.getMTLAddress().export()

    @staticmethod
    def parse(agentAddressRaw):
        return AgentAddress(getMTLImplementation().parseMTLAddress(agentAddressRaw))