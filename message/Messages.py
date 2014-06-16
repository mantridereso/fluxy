__author__ = 'martin'

import hashlib
import time
from agent.AbstractAgent import *


class AbstractMessage():
    KEY_HEADER = "header"
    KEY_PAYLOAD = "payload"

    MESSAGE_TYPE = "message"

    class AbstractHeader():

        KEY_MESSAGE_TYPE = "message_type"
        KEY_RECEIVER = "receiver"

        def __init__(self):
            self._messageType = None
            self._receiver = None

        def getMessageType(self):
            return self._messageType

        def setMessageType(self, messageType):
            self._messageType = messageType


        def getReceiver(self):
            """

            :rtype : AbstractAgentIdentity
            """
            if not isinstance(self._receiver, AbstractAgentIdentity):
                self._receiver = AbstractAgentIdentity.parse(self._receiver)
            return self._receiver

        def setReceiver(self, receiver):
            self._receiver = receiver

        def export(self):
            raise NotImplemented

        @staticmethod
        def parse(rawHeaderDict):
            """

            :rtype : AbstractMessage.AbstractHeader
            """
            raise NotImplemented

        def isWellFormed(self):
            """

            :rtype : bool
            """
            raise NotImplemented


    def __init__(self):
        self._header = None
        self._payload = dict()

    def _getHeader(self):
        raise NotImplemented

    def setHeader(self, header):
        raise NotImplemented

    def _getPayload(self):
        return self._payload

    def _setPayload(self, payloadDict):
        assert isinstance(payloadDict, dict)
        self._payload = payloadDict

    def _setPayloadEntry(self, key, value):
        self._payload[key] = value

    def _getPayloadEntry(self, key):
        if self._hasPayloadEntry(key):
            return self._payload[key]
        else:
            return None

    def _payloadIsEmpty(self):
        return len(self._payload) == 0

    def _hasPayloadEntry(self, key):
        return key in self._payload

    def _exportHeader(self):
        self._getHeader().setMessageType(self.MESSAGE_TYPE)
        return self._getHeader().export()

    def export(self):
        ret = {
            self.KEY_HEADER: self._exportHeader(),
            self.KEY_PAYLOAD: self._getPayload()
        }
        return ret

    def parsePayload(self, payload):
        raise NotImplemented


    def isWellFormed(self):
        return self._getHeader().isWellFormed() and self._payloadIsWellFormed()

    def _payloadIsWellFormed(self):
        raise NotImplemented


    def getReceiver(self):
        """

        :rtype : AbstractAgentIdentity
        """
        return self._getHeader().getReceiver()

    def setReceiver(self, receiver):
        self._getHeader().setReceiver(receiver)


class CommunicationMessage(AbstractMessage):

    MESSAGE_TYPE = "comm_message"

    class Header(AbstractMessage.AbstractHeader):

        KEY_SENDER = "sender"
        KEY_CONVERSATION_ID = "conversation_id"

        def __init__(self):
            AbstractMessage.AbstractHeader.__init__(self)
            self._sender = None
            self._conversationId = ""

        def getSender(self):
            """

            :rtype : AbstractAgentIdentity
            """
            if not isinstance(self._sender, AbstractAgentIdentity):
                self._sender = AbstractAgentIdentity.parse(self._sender)
            return self._sender

        def setSender(self, sender):
            self._sender = sender

        def getConversationId(self):
            return self._conversationId

        def setConversationId(self, conversationId):
            self._conversationId = conversationId

        def export(self):
            return {
                self.KEY_MESSAGE_TYPE: self.getMessageType(),
                self.KEY_CONVERSATION_ID: self.getConversationId(),
                self.KEY_SENDER: self.getSender().export(),
                self.KEY_RECEIVER: self.getReceiver().export()
            }

        @staticmethod
        def parse(rawHeaderDict):
            assert (isinstance(rawHeaderDict, dict))
            header = CommunicationMessage.Header()
            header.setMessageType(rawHeaderDict[CommunicationMessage.Header.KEY_MESSAGE_TYPE])
            header.setConversationId(rawHeaderDict[CommunicationMessage.Header.KEY_CONVERSATION_ID])
            header.setSender(rawHeaderDict[CommunicationMessage.Header.KEY_SENDER])
            header.setReceiver(rawHeaderDict[CommunicationMessage.Header.KEY_RECEIVER])
            return header

        def isWellFormed(self):
            return (self.getMessageType() is not None) \
                       and (self.getSender() is not None) \
                and (self.getReceiver() is not None)

    def _getHeader(self):
        if self._header is None:
            self._header = self.Header()
        return self._header

    def setHeader(self, header):
        assert (isinstance(header, self.Header))
        self._header = header

    def getSender(self):
        """

        :rtype : AbstractAgentIdentity
        """
        return self._getHeader().getSender()

    def setSender(self, sender):
        self._getHeader().setSender(sender)

    def getConversationId(self):
        return self._getHeader().getConversationId()

    def setConversationId(self, conversationId):
        self._getHeader().setConversationId(conversationId)


class RequestMessage(CommunicationMessage):
    MESSAGE_TYPE = "request"

    def __init__(self, payload=None):
        CommunicationMessage.__init__(self)
        if not payload is None:
            self._setPayload(payload)

    def _payloadIsWellFormed(self):
        return not self._payloadIsEmpty()

    def parsePayload(self, payload):
        assert isinstance(payload, dict)
        self._setPayload(payload)

    def autoGenerateConversationId(self):
        _hash = str(self)+str(time.time())
        md = hashlib.md5()
        md.update(_hash)
        hexSum = md.hexdigest()
        self._getHeader().setConversationId(hexSum)



class RequestReplyMessage(CommunicationMessage):
    MESSAGE_TYPE = "request_reply"

    def __init__(self, payload=None):
        CommunicationMessage.__init__(self)
        if not payload is None:
            self._setPayload(payload)

    def inReplyTo(self, originalMessage):
        """

        :type self: RequestReplyCommunicationMessage
        """
        assert (isinstance(originalMessage, RequestMessage))
        self._getHeader().setMessageType(self.MESSAGE_TYPE)
        self._getHeader().setSender(originalMessage.getReceiver())
        self._getHeader().setReceiver(originalMessage.getSender())
        self._getHeader().setConversationId(originalMessage.getConversationId())
        return self

    def _payloadIsWellFormed(self):
        return not self._payloadIsEmpty()

    def parsePayload(self, payload):
        assert isinstance(payload, dict)
        self._setPayload(payload)


class InformMessage(CommunicationMessage):
    MESSAGE_TYPE = "inform"

    def __init__(self, payload=None):
        CommunicationMessage.__init__(self)
        if not payload is None:
            self._setPayload(payload)

    def _payloadIsWellFormed(self):
        return not self._payloadIsEmpty()

    def parsePayload(self, payload):
        assert isinstance(payload, dict)
        self._setPayload(payload)


class PingMessage(RequestMessage):
    MESSAGE_TYPE = "ping"


class PongMessage(RequestReplyMessage):
    MESSAGE_TYPE = "pong"


class RoleRequestMessage(RequestMessage):
    MESSAGE_TYPE = "request_role"

    KEY_ROLE = "role"

    def __init__(self, roleName=None):
        if isinstance(roleName, AbstractAgentIdentity):
            roleName = roleName.getRole()
        RequestMessage.__init__(self)
        if roleName is not None:
            self._setPayloadEntry(self.KEY_ROLE, roleName)


    def getRoleName(self):
        return self._getPayloadEntry(self.KEY_ROLE)

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_ROLE)

    def parsePayload(self, payload):
        if self.KEY_ROLE in payload:
            self._setPayloadEntry(self.KEY_ROLE, payload[self.KEY_ROLE])


class RoleRequestSuccessMessage(RequestReplyMessage):
    MESSAGE_TYPE = "role_request_success"

    KEY_ROLE_ADDRESS = "role_address"

    def __init__(self, agent=None):
        RequestReplyMessage.__init__(self)
        from agent.Agent import ParticularAgent
        if isinstance(agent, ParticularAgent):
            self._setPayloadEntry(self.KEY_ROLE_ADDRESS, agent.getAddress().export())

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_ROLE_ADDRESS)

    def getRoleAddress(self):
        from agent.Agent import ParticularAgent, AgentAddress
        return ParticularAgent(AgentAddress.parse(self._getPayloadEntry(self.KEY_ROLE_ADDRESS)))

    def parsePayload(self, payload):
        if self.KEY_ROLE_ADDRESS in payload:
            self._setPayloadEntry(self.KEY_ROLE_ADDRESS, payload[self.KEY_ROLE_ADDRESS])


class RoleRequestFailedMessage(RequestReplyMessage):
    MESSAGE_TYPE = "role_request_failed"

    KEY_REASON = "reason"

    def __init__(self, reason="<unknown>", payload=None):
        #assert isinstance(reason, str)
        RequestReplyMessage.__init__(self, payload)
        self._setPayloadEntry(self.KEY_REASON, reason)

    def _payloadIsWellFormed(self):
        return True

    def parsePayload(self, payload):
        if self.KEY_REASON in payload:
            self._setPayloadEntry(self.KEY_REASON, payload[self.KEY_REASON])


class GetGroupManagerAddressRequestMessage(RequestMessage):
    MESSAGE_TYPE = "get_group_manager"

    KEY_GROUP = "group"

    def __init__(self, groupName=None):
        RequestMessage.__init__(self)
        if not groupName is None:
            self._setPayloadEntry(self.KEY_GROUP, groupName)

    def getGroupName(self):
        return self._getPayloadEntry(self.KEY_GROUP)

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_GROUP)

    def parsePayload(self, payload):
        if self.KEY_GROUP in payload:
            self._setPayloadEntry(self.KEY_GROUP, payload[self.KEY_GROUP])


class GroupManagerFoundReplyMessage(RequestReplyMessage):
    MESSAGE_TYPE = "group_manager_found"

    KEY_GROUP_MANAGER = "group_manager"

    def __init__(self, groupManager=None):
        RequestReplyMessage.__init__(self)
        if not groupManager is None:
            from agent.Agent import AbstractAgentIdentity
            if isinstance(groupManager, AbstractAgentIdentity):
                groupManager = groupManager.export()
            self._setPayloadEntry(self.KEY_GROUP_MANAGER, groupManager)

    def getGroupManager(self):
        from agent.Agent import AbstractAgentIdentity
        return AbstractAgentIdentity.parse(self._getPayloadEntry(self.KEY_GROUP_MANAGER))

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_GROUP_MANAGER)

    def parsePayload(self, payload):
        if self.KEY_GROUP_MANAGER in payload:
            self._setPayloadEntry(self.KEY_GROUP_MANAGER, payload[self.KEY_GROUP_MANAGER])


class GroupManagerNotFoundReplyMessage(RequestReplyMessage):
    MESSAGE_TYPE = "group_manager_not_found"

    def _payloadIsWellFormed(self):
        return True


class FindAgentsWithRoleMessage(RequestMessage):
    MESSAGE_TYPE = "find_agents_with_role"

    KEY_ROLE_NAME = "role"

    def __init__(self, roleName=None):
        RequestMessage.__init__(self)
        if not roleName is None:
            self._setPayloadEntry(self.KEY_ROLE_NAME, roleName)

    def getRoleName(self):
        return self._getPayloadEntry(self.KEY_ROLE_NAME)

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_ROLE_NAME)

    def parsePayload(self, payload):
        if self.KEY_ROLE_NAME in payload:
            self._setPayloadEntry(self.KEY_ROLE_NAME, payload[self.KEY_ROLE_NAME])


class FoundAgentsWithRoleMessage(RequestReplyMessage):
    MESSAGE_TYPE = "found_agents_with_role"

    KEY_ACTORS = "agents"

    def __init__(self, agentList=None):
        RequestReplyMessage.__init__(self)
        if not agentList is None:
            assert isinstance(agentList, list)
            agentListExported = []
            for agent in agentList:
                assert isinstance(agent, AbstractAgentIdentity)
                agentListExported.append(agent.export())
            self._setPayloadEntry(self.KEY_ACTORS, agentListExported)

    def getAgents(self):
        agentsRaw = self._getPayloadEntry(self.KEY_ACTORS)
        assert isinstance(agentsRaw, list)
        agents = [AbstractAgentIdentity.parse(x) for x in agentsRaw]
        return agents

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_ACTORS)

    def parsePayload(self, payload):
        if self.KEY_ACTORS in payload:
            self._setPayloadEntry(self.KEY_ACTORS, payload[self.KEY_ACTORS])
