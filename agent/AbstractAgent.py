__author__ = 'martin'

import hashlib
import json

from MTL import *
from Address import *


class AbstractAgentIdentity():

    KEY_ACTOR = "agent"
    KEY_GROUP = "group"
    KEY_ROLE = "role"

    def __init__(self, abstractAgent, roleName, groupName):
        """

        :param abstractAgent: AbstractAgent
        :param roleName: str
        :param groupName: str
        """
        assert isinstance(abstractAgent, AbstractAgent)
        #assert isinstance(roleName, basestring)
        #assert isinstance(groupName, basestring)
        self._agent = abstractAgent
        self._roleName = roleName
        self._groupName = groupName
        self._hashKey = None

    def getRole(self):
        """

        :rtype : basestring
        """
        return self._roleName

    def getGroup(self):
        """

        :rtype : basestring
        """
        return self._groupName

    def getAgent(self):
        """

        :rtype : AbstractAgent
        """
        return self._agent

    def export(self):
        """

        :rtype : dict
        """
        return {
            self.KEY_ACTOR: self.getAgent().exportId(),
            self.KEY_GROUP: self.getGroup(),
            self.KEY_ROLE: self.getRole()
        }

    def getHashKey(self):
        if self._hashKey is None:
            strSelf = json.dumps(self.export())
            md = hashlib.md5()
            md.update(strSelf)
            self._hashKey = md.hexdigest()
        return self._hashKey

    @staticmethod
    def parse(agentIdentityRawDict):
        assert(isinstance(agentIdentityRawDict, dict))
        if agentIdentityRawDict[AbstractAgentIdentity.KEY_ACTOR] == AnyAgent.ID:
            agent = AnyAgent()
        elif agentIdentityRawDict[AbstractAgentIdentity.KEY_ACTOR] == AllAgent.ID:
            agent = AllAgent()
        else:
            agent = ParticularAgent(AgentAddress.parse(agentIdentityRawDict[AbstractAgentIdentity.KEY_ACTOR]))
        groupName = agentIdentityRawDict[AbstractAgentIdentity.KEY_GROUP]
        roleName = agentIdentityRawDict[AbstractAgentIdentity.KEY_ROLE]
        return AbstractAgentIdentity(agent, roleName, groupName)


class AbstractAgent():

    class _IdentityBuilder():
        def __init__(self, agent, roleName):
            self._agent = agent
            self._roleName = roleName

        def withinGroup(self, groupNameOrGroup):
            groupName = groupNameOrGroup
            #if isinstance(groupNameOrGroup, Group):
            #    groupName = groupNameOrGroup.getName()
                #assert isinstance(groupName, basestring)
            return AbstractAgentIdentity(self._agent, self._roleName, groupName)

    def __init__(self):
        pass

    def inRole(self, roleName):
        #assert (isinstance(roleName, basestring))
        return self._IdentityBuilder(self, roleName)

    def exportId(self):
        raise NotImplemented


class AnyAgent(AbstractAgent):
    ID = "__ANY__"

    def exportId(self):
        return self.ID


class AllAgent(AbstractAgent):
    ID = "__ALL__"

    def exportId(self):
        return self.ID


class ParticularAgent(AbstractAgent):

    def __init__(self, agentAddress=None):
        AbstractAgent.__init__(self)
        assert(isinstance(agentAddress, AgentAddress) or (agentAddress is None))
        self._agentAddress = agentAddress
        self._hashKey = None

    def getAddress(self):
        """


        :raise NotImplemented:
        :rtype AgentAddress
        """
        assert isinstance(self._agentAddress, AgentAddress)
        return self._agentAddress

    def exportId(self):
        return self.getAddress().export()

    def __str__(self):
        return self.getAddress().exportId()

    def getHashKey(self):
        if self._hashKey is None:
            hashBase = str(self)
            md = hashlib.md5()
            md.update(hashBase)
            self._hashKey = md.hexdigest()
        return self._hashKey


